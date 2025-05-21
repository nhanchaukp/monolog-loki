<?php

/*
 * Copyright (c) 2016 - 2022 Itspire.
 * This software is licensed under the BSD-3-Clause license. (see LICENSE.md for full license)
 * All Right Reserved.
 */

declare(strict_types=1);

namespace Itspire\MonologLoki\Handler;

use Itspire\MonologLoki\Formatter\LokiFormatter;
use Monolog\Formatter\FormatterInterface;
use Monolog\Handler\AbstractProcessingHandler;
use Monolog\Handler\Curl;
use Monolog\Level;
use Monolog\LogRecord;

class LokiHandler extends AbstractProcessingHandler
{
    /** the scheme, hostname and port to the Loki system */
    protected ?string $entrypoint;

    /** the identifiers for Basic Authentication to the Loki system */
    protected array $basicAuth = [];

    /** the name of the system (hostname) sending log messages to Loki */
    protected ?string $systemName;

    /** the list of default context variables to be sent to the Loki system */
    protected array $globalContext = [];

    /** the list of default labels to be sent to the Loki system */
    protected array $globalLabels = [];

    /** custom curl options */
    protected array $customCurlOptions = [];

    /** curl options which cannot be customized */
    protected array $nonCustomizableCurlOptions = [
        CURLOPT_CUSTOMREQUEST,
        CURLOPT_RETURNTRANSFER,
        CURLOPT_POSTFIELDS,
        CURLOPT_HTTPHEADER,
    ];

    /** @var false|null|resource */
    private $connection;

    /** @var string|null tenant id (HTTP header X-Scope-OrgID), if null -> no header */
    protected ?string $tenantId = null;

    public function __construct(array $apiConfig, int|string|Level $level = Level::Debug, bool $bubble = true)
    {
        if (!function_exists('json_encode')) {
            throw new \RuntimeException('PHP\'s json extension is required to use Monolog\'s LokiHandler');
        }
        parent::__construct($level, $bubble);
        $this->entrypoint = $this->getEntrypoint($apiConfig['entrypoint']);
        $this->globalContext = $apiConfig['context'] ?? [];
        $this->globalLabels = $apiConfig['labels'] ?? [];
        $this->systemName = $apiConfig['client_name'] ?? null;
        $this->customCurlOptions = $this->determineValidCustomCurlOptions($apiConfig['curl_options'] ?? []);
        if (isset($apiConfig['auth']['basic'])) {
            $this->basicAuth = (2 === count($apiConfig['auth']['basic'])) ? $apiConfig['auth']['basic'] : [];
        }
        if (isset($apiConfig['tenant_id'])) {
            $this->tenantId = $apiConfig['tenant_id'];
            $this->globalContext['tenantId'] = $apiConfig['tenant_id'];
        }
    }

    private function determineValidCustomCurlOptions(array $configuredCurlOptions): array
    {
        foreach ($this->nonCustomizableCurlOptions as $option) {
            unset($configuredCurlOptions[$option]);
        }

        return $configuredCurlOptions;
    }

    private function getEntrypoint(string $entrypoint): string
    {
        if ('/' !== substr($entrypoint, -1)) {
            return $entrypoint;
        }

        return substr($entrypoint, 0, -1);
    }

    /** @throws \JsonException */
    public function handleBatch(array $records): void
    {
        $rows = [];
        foreach ($records as $record) {
            if (!$this->isHandling($record)) {
                continue;
            }

            $record = $this->processRecord($record);
            $rows[] = $this->getFormatter()->format($record);
        }

        $this->sendPacket(['streams' => $rows]);
    }

    /** @throws \JsonException */
    protected function sendPacket(array $packet): void {
        $payload = json_encode($packet, JSON_THROW_ON_ERROR | JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);

        $url = sprintf('%s/loki/api/v1/push', $this->entrypoint);
        $env_vars = [
            'LOKI_URL' => $url,
            'LOKI_AUTH' => implode(':', $this->basicAuth),
            'PAYLOAD' => $payload,
            'CONTENT_LENGTH' => strlen($payload),
        ];

        $process = \Symfony\Component\Process\Process::fromShellCommandline('curl -u "$LOKI_AUTH" -H "Content-Type: application/json" -H "Content-Length: $CONTENT_LENGTH" -d "$PAYLOAD" -X POST $LOKI_URL', NULL, $env_vars);
        $process->run();
    }

    protected function getDefaultFormatter(): FormatterInterface
    {
        return new LokiFormatter($this->globalLabels, $this->globalContext, $this->systemName);
    }

    /** @throws \JsonException */
    protected function write(LogRecord $record): void
    {
        $this->sendPacket(['streams' => [$this->getFormatter()->format($record)]]);
    }
}
