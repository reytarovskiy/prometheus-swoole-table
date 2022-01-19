Swoole table storage adapter for PHP Prometheus client

## Install
```bash
composer require reytarovskiy/prometheus-swoole-table
```

## Usage
```php
$storage = new \Reytarovskiy\PrometheusSwooleTable\SwooleTable();
$registry = new \Prometheus\CollectorRegistry\CollectorRegistry($storage, false);

// use registry for register metrics
```
