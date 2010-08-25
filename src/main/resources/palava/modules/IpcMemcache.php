<?php

/**
 * Looks for cached commands in a memcached filled by palava-ipc-memcached.
 */
class IpcMemcache extends AbstractPalavaModule {

    const NAME = __CLASS__;

    const CONFIG_ENABLED = "memcache.enabled";
    const CONFIG_ADDRESSES = "memcache.addresses";
    const DEFAULT_ENABLED = true;

    /**
     * @var Memcache
     */
    private $connection = null;

    /**
     * statistics
     */
    private $statistics = array();

    private function enabled() {
        return $this->get(IpcMemcache::CONFIG_ENABLED, IpcMemcache::DEFAULT_ENABLED);
    }

    private function connect() {
        if (!is_null($this->connection)) {
            return;
        }

        $addresses = $this->get(IpcMemcache::CONFIG_ADDRESSES);
        if (!$addresses) {
            throw new Exception("memcached addresses not configured [".IpcMemcache::CONFIG_ADDRESSES."]");
        }

        $this->connection = new Memcache();

        $addrs = explode(' ', $addresses);
        foreach ($addrs as $addr) {
            list($host, $port) = explode(':', $addr);
            $this->connection->addserver($host, $port);
        }
    }

    private function callKey(&$call) {
        $key = array(
            'cmd' => $call[Palava::PKEY_COMMAND],
            'args' => $call[Palava::PKEY_ARGUMENTS]
        );
        return json_encode($key);
    }

    public function preCall(&$call) {
        // module disabled?
        if (!$this->enabled()) {
            return null;
        }
        $this->connect();

        // the json key within the memcached
        $key = $this->callKey($call);

        // get it
        $time_start = microtime(true);
        $json = $this->connection->get($key);
        if (strlen($json) >= 2) {  /* minimum json: {} */
            // return something which looks like a real response
            $response = array(
                Palava::PKEY_PROTOCOL => $call[Palava::PKEY_PROTOCOL],
                Palava::PKEY_SESSION => $call[Palava::PKEY_SESSION],
                Palava::PKEY_RESULT => json_decode($json, true)
            );
            $this->stats($time_start, $call, true, $key);
            return $response;
        } else {
            $this->stats($time_start, $call, false, $key);
            return null;
        }
    }

    private function stats($starttime, &$call, $cached, $key) {
        $endtime = microtime(true);
        $duration = $endtime - $starttime;

        $this->statistics[] = array(
            'call' => $call,
            'duration' => $duration,
            'key' => $key,
            'cached' => $cached,
        );
    }

    public function getStatistics() {
        return $this->statistics;
    }
}
