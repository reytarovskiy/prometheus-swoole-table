<?php

namespace Reytarovskiy\PrometheusSwooleTable;

use Prometheus\Counter;
use Prometheus\Gauge;
use Prometheus\Histogram;
use Prometheus\Math;
use Prometheus\MetricFamilySamples;
use Prometheus\Storage\Adapter;
use Prometheus\Summary;
use RuntimeException;
use Swoole\Lock;
use Swoole\Table;

class SwooleTable implements Adapter
{
    private const DATA_FIELD = 'data';
    private const ORIGINAL_KEY_FIELD = 'original_key';

    /** @var Table */
    private $counters;

    /** @var Table */
    private $gauges;

    /** @var Table */
    private $histograms;

    /** @var Table */
    private $summaries;

    /** @var Lock */
    private $histogramsLock;

    /** @var Lock */
    private $gaugesLock;

    /** @var Lock */
    private $summariesLock;

    /** @var Lock */
    private $countersLock;

    /** @var int */
    private $tableSize;

    /** @var int */
    private $valueSize;

    /** @var int */
    private $originalKeyLength;

    public function __construct(
        int $tableSize = 1024,
        int $valueSize = 1024,
        int $originalKeyLength = 256
    ) {
        $this->histograms = $this->createTable($tableSize, $valueSize, $originalKeyLength);
        $this->gauges = $this->createTable($tableSize, $valueSize, $originalKeyLength);
        $this->summaries = $this->createTable($tableSize, $valueSize, $originalKeyLength);
        $this->counters = $this->createTable($tableSize, $valueSize, $originalKeyLength);

        $this->histogramsLock = new Lock(SWOOLE_MUTEX);
        $this->gaugesLock = new Lock(SWOOLE_MUTEX);
        $this->summariesLock = new Lock(SWOOLE_MUTEX);
        $this->countersLock = new Lock(SWOOLE_MUTEX);
        $this->tableSize = $tableSize;
        $this->valueSize = $valueSize;
        $this->originalKeyLength = $originalKeyLength;
    }

    private function createTable(int $tableSize, int $valueSize, int $originalKeyLength): Table
    {
        $table = new Table($tableSize);
        $table->column(self::DATA_FIELD, Table::TYPE_STRING, $valueSize);
        $table->column(self::ORIGINAL_KEY_FIELD, Table::TYPE_STRING, $originalKeyLength);
        $table->create();

        return $table;
    }

    /**
     * @return MetricFamilySamples[]
     */
    public function collect(): array
    {
        $metrics = $this->internalCollect($this->getAllFromTable($this->counters));
        $metrics = array_merge($metrics, $this->internalCollect($this->getAllFromTable($this->gauges)));
        $metrics = array_merge($metrics, $this->collectHistograms());
        $metrics = array_merge($metrics, $this->collectSummaries());
        return $metrics;
    }

    /**
     * @deprecated use replacement method wipeStorage from Adapter interface
     */
    public function flushMemory(): void
    {
        $this->wipeStorage();
    }

    /**
     * @inheritDoc
     */
    public function wipeStorage(): void
    {
        $this->counters->destroy();
        $this->counters = $this->createTable($this->tableSize, $this->valueSize, $this->originalKeyLength);

        $this->gauges->destroy();
        $this->gauges = $this->createTable($this->tableSize, $this->valueSize, $this->originalKeyLength);

        $this->histograms->destroy();
        $this->histograms = $this->createTable($this->tableSize, $this->valueSize, $this->originalKeyLength);

        $this->summaries->destroy();
        $this->summaries = $this->createTable($this->tableSize, $this->valueSize, $this->originalKeyLength);
    }

    /**
     * @return MetricFamilySamples[]
     */
    private function collectHistograms(): array
    {
        $histograms = [];
        foreach ($this->getAllFromTable($this->histograms) as $histogram) {
            $metaData = $histogram['meta'];
            $data = [
                'name' => $metaData['name'],
                'help' => $metaData['help'],
                'type' => $metaData['type'],
                'labelNames' => $metaData['labelNames'],
                'buckets' => $metaData['buckets'],
            ];

            // Add the Inf bucket so we can compute it later on
            $data['buckets'][] = '+Inf';

            $histogramBuckets = [];
            foreach ($histogram['samples'] as $key => $value) {
                $parts = explode(':', $key);
                $labelValues = $parts[2];
                $bucket = $parts[3];
                // Key by labelValues
                $histogramBuckets[$labelValues][$bucket] = $value;
            }

            // Compute all buckets
            $labels = array_keys($histogramBuckets);
            sort($labels);
            foreach ($labels as $labelValues) {
                $acc = 0;
                $decodedLabelValues = $this->decodeLabelValues($labelValues);
                foreach ($data['buckets'] as $bucket) {
                    $bucket = (string)$bucket;
                    if (!isset($histogramBuckets[$labelValues][$bucket])) {
                        $data['samples'][] = [
                            'name' => $metaData['name'] . '_bucket',
                            'labelNames' => ['le'],
                            'labelValues' => array_merge($decodedLabelValues, [$bucket]),
                            'value' => $acc,
                        ];
                    } else {
                        $acc += $histogramBuckets[$labelValues][$bucket];
                        $data['samples'][] = [
                            'name' => $metaData['name'] . '_' . 'bucket',
                            'labelNames' => ['le'],
                            'labelValues' => array_merge($decodedLabelValues, [$bucket]),
                            'value' => $acc,
                        ];
                    }
                }

                // Add the count
                $data['samples'][] = [
                    'name' => $metaData['name'] . '_count',
                    'labelNames' => [],
                    'labelValues' => $decodedLabelValues,
                    'value' => $acc,
                ];

                // Add the sum
                $data['samples'][] = [
                    'name' => $metaData['name'] . '_sum',
                    'labelNames' => [],
                    'labelValues' => $decodedLabelValues,
                    'value' => $histogramBuckets[$labelValues]['sum'],
                ];
            }
            $histograms[] = new MetricFamilySamples($data);
        }
        return $histograms;
    }

    /**
     * @return MetricFamilySamples[]
     */
    private function collectSummaries(): array
    {
        $math = new Math();
        $summaries = [];
        foreach ($this->getAllFromTable($this->summaries) as $metaKey => &$summary) {
            $metaData = $summary['meta'];
            $data = [
                'name' => $metaData['name'],
                'help' => $metaData['help'],
                'type' => $metaData['type'],
                'labelNames' => $metaData['labelNames'],
                'maxAgeSeconds' => $metaData['maxAgeSeconds'],
                'quantiles' => $metaData['quantiles'],
                'samples' => [],
            ];

            foreach ($summary['samples'] as $key => &$values) {
                $parts = explode(':', $key);
                $labelValues = $parts[2];
                $decodedLabelValues = $this->decodeLabelValues($labelValues);

                // Remove old data
                $values = array_filter($values, function (array $value) use ($data): bool {
                    return time() - $value['time'] <= $data['maxAgeSeconds'];
                });
                if (count($values) === 0) {
                    unset($summary['samples'][$key]);
                    continue;
                }

                // Compute quantiles
                usort($values, function (array $value1, array $value2) {
                    if ($value1['value'] === $value2['value']) {
                        return 0;
                    }
                    return ($value1['value'] < $value2['value']) ? -1 : 1;
                });

                foreach ($data['quantiles'] as $quantile) {
                    $data['samples'][] = [
                        'name' => $metaData['name'],
                        'labelNames' => ['quantile'],
                        'labelValues' => array_merge($decodedLabelValues, [$quantile]),
                        'value' => $math->quantile(array_column($values, 'value'), $quantile),
                    ];
                }

                // Add the count
                $data['samples'][] = [
                    'name' => $metaData['name'] . '_count',
                    'labelNames' => [],
                    'labelValues' => $decodedLabelValues,
                    'value' => count($values),
                ];

                // Add the sum
                $data['samples'][] = [
                    'name' => $metaData['name'] . '_sum',
                    'labelNames' => [],
                    'labelValues' => $decodedLabelValues,
                    'value' => array_sum(array_column($values, 'value')),
                ];
            }
            if (count($data['samples']) > 0) {
                $summaries[] = new MetricFamilySamples($data);
            } else {
                $this->deleteFromTable($this->summaries, $metaKey);
            }
        }
        return $summaries;
    }

    /**
     * @param mixed[] $metrics
     * @return MetricFamilySamples[]
     */
    private function internalCollect(array $metrics): array
    {
        $result = [];
        foreach ($metrics as $metric) {
            $metaData = $metric['meta'];
            $data = [
                'name' => $metaData['name'],
                'help' => $metaData['help'],
                'type' => $metaData['type'],
                'labelNames' => $metaData['labelNames'],
                'samples' => [],
            ];
            foreach ($metric['samples'] as $key => $value) {
                $parts = explode(':', $key);
                $labelValues = $parts[2];
                $data['samples'][] = [
                    'name' => $metaData['name'],
                    'labelNames' => [],
                    'labelValues' => $this->decodeLabelValues($labelValues),
                    'value' => $value,
                ];
            }
            $this->sortSamples($data['samples']);
            $result[] = new MetricFamilySamples($data);
        }
        return $result;
    }

    /**
     * @param mixed[] $data
     * @return void
     */
    public function updateHistogram(array $data): void
    {
        $this->histogramsLock->lock();
        // Initialize the sum
        $metaKey = $this->metaKey($data);

        $histogram = $this->getFromTable($this->histograms, $metaKey);
        if ($histogram === false) {
            $histogram = [
                'meta' => $this->metaData($data),
                'samples' => [],
            ];
        }

        $sumKey = $this->histogramBucketValueKey($data, 'sum');
        if (array_key_exists($sumKey, $histogram['samples']) === false) {
            $histogram['samples'][$sumKey] = 0;
        }

        $histogram['samples'][$sumKey] += $data['value'];

        $bucketToIncrease = '+Inf';
        foreach ($data['buckets'] as $bucket) {
            if ($data['value'] <= $bucket) {
                $bucketToIncrease = $bucket;
                break;
            }
        }

        $bucketKey = $this->histogramBucketValueKey($data, $bucketToIncrease);
        if (array_key_exists($bucketKey, $histogram['samples']) === false) {
            $histogram['samples'][$bucketKey] = 0;
        }
        $histogram['samples'][$bucketKey] += 1;

        $this->updateInTable($this->histograms, $metaKey, $histogram);
        $this->histogramsLock->unlock();
    }

    /**
     * @param mixed[] $data
     * @return void
     */
    public function updateSummary(array $data): void
    {
        $this->summariesLock->lock();

        $metaKey = $this->metaKey($data);
        $summary = $this->getFromTable($this->summaries, $metaKey);
        if ($summary === false) {
            $summary = [
                'meta' => $this->metaData($data),
                'samples' => [],
            ];
        }

        $valueKey = $this->valueKey($data);
        if (array_key_exists($valueKey, $summary['samples']) === false) {
            $summary['samples'][$valueKey] = [];
        }

        $summary['samples'][$valueKey][] = [
            'time' => time(),
            'value' => $data['value'],
        ];

        $this->updateInTable($this->summaries, $metaKey, $summary);
        $this->summariesLock->unlock();
    }

    /**
     * @param mixed[] $data
     */
    public function updateGauge(array $data): void
    {
        $this->gaugesLock->lock();

        $metaKey = $this->metaKey($data);
        $gauge = $this->getFromTable($this->gauges, $metaKey);
        if ($gauge === false) {
            $gauge = [
                'meta' => $this->metaData($data),
                'samples' => [],
            ];
        }

        $valueKey = $this->valueKey($data);
        if (array_key_exists($valueKey, $gauge['samples']) === false) {
            $gauge['samples'][$valueKey] = 0;
        }
        if ($data['command'] === Adapter::COMMAND_SET) {
            $gauge['samples'][$valueKey] = $data['value'];
        } else {
            $gauge['samples'][$valueKey] += $data['value'];
        }

        $this->updateInTable($this->gauges, $metaKey, $gauge);
        $this->gaugesLock->unlock();
    }

    /**
     * @param mixed[] $data
     */
    public function updateCounter(array $data): void
    {
        $this->countersLock->lock();

        $metaKey = $this->metaKey($data);
        $counter = $this->getFromTable($this->counters, $metaKey);
        if ($counter === false) {
            $counter = [
                'meta' => $this->metaData($data),
                'samples' => [],
            ];
        }

        $valueKey = $this->valueKey($data);
        if (array_key_exists($valueKey, $counter['samples']) === false) {
            $counter['samples'][$valueKey] = 0;
        }
        if ($data['command'] === Adapter::COMMAND_SET) {
            $counter['samples'][$valueKey] = 0;
        } else {
            $counter['samples'][$valueKey] += $data['value'];
        }

        $this->updateInTable($this->counters, $metaKey, $counter);
        $this->countersLock->unlock();
    }

    public function deleteMetric(string $type, string $name): void
    {
        $metaKey = $this->metaKey(compact('type', 'name'));

        $tables = [
            Counter::TYPE => $this->counters,
            Histogram::TYPE => $this->histograms,
            Gauge::TYPE => $this->gauges,
            Summary::TYPE => $this->summaries,
        ];

        $this->deleteFromTable($tables[$type], $metaKey);
    }

    /**
     * @param mixed[]    $data
     * @param string|int $bucket
     *
     * @return string
     */
    private function histogramBucketValueKey(array $data, $bucket): string
    {
        return implode(':', [
            $data['type'],
            $data['name'],
            $this->encodeLabelValues($data['labelValues']),
            $bucket,
        ]);
    }

    /**
     * @param mixed[] $data
     *
     * @return string
     */
    private function metaKey(array $data): string
    {
        return implode(':', [
            $data['type'],
            $data['name'],
            'meta'
        ]);
    }

    /**
     * @param mixed[] $data
     *
     * @return string
     */
    private function valueKey(array $data): string
    {
        return implode(':', [
            $data['type'],
            $data['name'],
            $this->encodeLabelValues($data['labelValues']),
            'value'
        ]);
    }

    /**
     * @param mixed[] $data
     *
     * @return mixed[]
     */
    private function metaData(array $data): array
    {
        $metricsMetaData = $data;
        unset($metricsMetaData['value'], $metricsMetaData['command'], $metricsMetaData['labelValues']);
        return $metricsMetaData;
    }

    /**
     * @param mixed[] $samples
     */
    private function sortSamples(array &$samples): void
    {
        usort($samples, function ($a, $b): int {
            return strcmp(implode("", $a['labelValues']), implode("", $b['labelValues']));
        });
    }

    /**
     * @param mixed[] $values
     * @return string
     * @throws RuntimeException
     */
    private function encodeLabelValues(array $values): string
    {
        $json = json_encode($values);
        if (false === $json) {
            throw new RuntimeException(json_last_error_msg());
        }
        return base64_encode($json);
    }

    /**
     * @param string $values
     * @return mixed[]
     * @throws RuntimeException
     */
    private function decodeLabelValues(string $values): array
    {
        $json = base64_decode($values, true);
        if (false === $json) {
            throw new RuntimeException('Cannot base64 decode label values');
        }
        $decodedValues = json_decode($json, true);
        if (false === $decodedValues) {
            throw new RuntimeException(json_last_error_msg());
        }
        return $decodedValues;
    }

    private function getAllFromTable(Table $table): array
    {
        $items = [];
        foreach ($table as $metaKey => $item) {
            $originalKey = $item[self::ORIGINAL_KEY_FIELD] ?? $metaKey;
            $items[$originalKey] = unserialize($item[self::DATA_FIELD], ['allowed_classes' => false]);
        }

        return $items;
    }

    private function getFromTable(Table $table, string $key)
    {
        $item = $table->get($this->keyHash($key), self::DATA_FIELD);

        return $item === false ? false : unserialize($item, ['allowed_classes' => false]);
    }

    private function updateInTable(Table $table, string $key, array $data): void
    {
        $table->set($this->keyHash($key), [
            self::DATA_FIELD => serialize($data),
            self::ORIGINAL_KEY_FIELD => $key,
        ]);
    }

    private function deleteFromTable(Table $table, string $metaKey): void
    {
        $table->del($this->keyHash($metaKey));
    }

    private function keyHash(string $originalKey): string
    {
        return hash('sha1', $originalKey);
    }
}
