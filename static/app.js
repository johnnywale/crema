// Crema Dashboard Application

document.addEventListener('alpine:init', () => {
    Alpine.data('dashboard', () => ({
        // Connection state
        connected: false,
        eventSource: null,

        // Cluster state
        nodeId: 0,
        isLeader: false,
        leaderId: null,
        term: 0,
        raftPeerCount: 0,
        commitIndex: 0,
        appliedIndex: 0,

        // Cache stats
        entryCount: 0,
        hitRate: 0,
        hits: 0,
        misses: 0,

        // Metrics
        getTotal: 0,
        putTotal: 0,
        putSuccess: 0,
        putFailures: 0,
        avgGetLatencyMs: 0,
        avgPutLatencyMs: 0,
        getLatencyP50: 0,
        getLatencyP90: 0,
        getLatencyP99: 0,
        putLatencyP50: 0,
        putLatencyP90: 0,
        putLatencyP99: 0,

        // Multi-Raft
        multiraftEnabled: false,
        totalShards: 0,
        activeShards: 0,
        localLeaderShards: 0,
        operationsPerSec: 0,
        totalEntries: 0,
        shards: [],

        // Slot routing
        slotRoutingEnabled: false,
        slotEpoch: 0,
        totalSlots: 1024,
        activeMigrations: 0,
        completedMigrations: 0,
        failedMigrations: 0,
        totalKeysMigrated: 0,

        // Shard management
        shardOperationLoading: false,
        showRemoveDialog: false,
        removeShardTarget: null,

        // Cache operations form
        opKey: '',
        opValue: '',
        opTtl: '',
        opResult: null,
        opResultType: 'info',
        opLoading: false,

        // Initialize
        init() {
            this.connect();
            this.fetchInitialData();
        },

        // Connect to SSE endpoint
        connect() {
            if (this.eventSource) {
                this.eventSource.close();
            }

            this.eventSource = new EventSource('/api/events');

            this.eventSource.onopen = () => {
                console.log('SSE connected');
                this.connected = true;
            };

            this.eventSource.onerror = (e) => {
                console.error('SSE error:', e);
                this.connected = false;
                // Reconnect after 3 seconds
                setTimeout(() => this.connect(), 3000);
            };

            this.eventSource.addEventListener('metrics', (e) => {
                const data = JSON.parse(e.data);
                this.updateMetrics(data);
            });

            this.eventSource.addEventListener('cluster_status', (e) => {
                const data = JSON.parse(e.data);
                this.updateClusterStatus(data);
            });

            this.eventSource.addEventListener('shard_update', (e) => {
                const data = JSON.parse(e.data);
                this.updateShards(data);
            });
        },

        // Fetch initial data
        async fetchInitialData() {
            try {
                // Fetch cluster status
                const statusRes = await fetch('/api/cluster/status');
                const status = await statusRes.json();
                this.updateClusterStatus(status);

                // Fetch metrics
                const metricsRes = await fetch('/api/metrics');
                const metrics = await metricsRes.json();
                this.updateMetrics(metrics);

                // Fetch multiraft stats
                const multiraftRes = await fetch('/api/multiraft/stats');
                const multiraft = await multiraftRes.json();
                this.multiraftEnabled = multiraft.enabled;
                this.totalShards = multiraft.total_shards;
                this.activeShards = multiraft.active_shards;
                this.localLeaderShards = multiraft.local_leader_shards;
                this.operationsPerSec = multiraft.operations_per_sec;

                // Fetch shards
                if (this.multiraftEnabled) {
                    const shardsRes = await fetch('/api/multiraft/shards');
                    const shardsData = await shardsRes.json();
                    this.shards = shardsData.shards;
                }

                // Fetch slot routing status
                await this.fetchSlotStatus();
            } catch (err) {
                console.error('Failed to fetch initial data:', err);
            }
        },

        // Fetch slot routing status
        async fetchSlotStatus() {
            try {
                const res = await fetch('/api/multiraft/slots/status');
                const data = await res.json();
                this.slotRoutingEnabled = data.enabled;
                this.slotEpoch = data.epoch;
                this.totalSlots = data.total_slots;
                this.activeMigrations = data.active_migrations;
                this.completedMigrations = data.completed_migrations;
                this.failedMigrations = data.failed_migrations;
                this.totalKeysMigrated = data.total_keys_migrated;
            } catch (err) {
                console.error('Failed to fetch slot status:', err);
            }
        },

        // Update handlers
        updateMetrics(data) {
            this.getTotal = data.get_total;
            this.hits = data.get_hits;
            this.misses = data.get_misses;
            this.hitRate = data.hit_rate;
            this.putTotal = data.put_total;
            this.putSuccess = data.put_success;
            this.putFailures = data.put_failures;
            this.entryCount = data.cache_entries;
            this.isLeader = data.is_leader;
            this.term = data.raft_term;
            this.avgGetLatencyMs = data.avg_get_latency_ms;
            this.avgPutLatencyMs = data.avg_put_latency_ms;
            this.getLatencyP50 = data.get_latency_p50_ms;
            this.getLatencyP90 = data.get_latency_p90_ms;
            this.getLatencyP99 = data.get_latency_p99_ms;
            this.putLatencyP50 = data.put_latency_p50_ms;
            this.putLatencyP90 = data.put_latency_p90_ms;
            this.putLatencyP99 = data.put_latency_p99_ms;
        },

        updateClusterStatus(data) {
            this.nodeId = data.node_id;
            this.leaderId = data.leader_id;
            this.isLeader = data.is_leader;
            this.term = data.term;
            this.raftPeerCount = data.raft_peer_count;
            this.commitIndex = data.commit_index;
            this.appliedIndex = data.applied_index;
        },

        updateShards(data) {
            this.shards = data.shards;
            this.totalShards = data.total_shards;
            this.totalEntries = data.total_entries || 0;
        },

        // Cache operations
        async doGet() {
            if (!this.opKey) {
                this.showResult('Please enter a key', 'error');
                return;
            }

            this.opLoading = true;
            try {
                const res = await fetch(`/api/cache/get/${encodeURIComponent(this.opKey)}`);
                const data = await res.json();

                if (data.success) {
                    if (data.value !== null && data.value !== undefined) {
                        const shardInfo = data.shard_id !== undefined ? ` (shard ${data.shard_id})` : '';
                        const latency = data.latency_ms ? `, ${data.latency_ms.toFixed(2)}ms` : '';
                        this.showResult(`GET "${this.opKey}" = "${data.value}"${shardInfo}${latency}`, 'success');
                    } else {
                        this.showResult(`GET "${this.opKey}" = (not found)`, 'info');
                    }
                } else {
                    this.showResult(`GET failed: ${data.error}`, 'error');
                }
            } catch (err) {
                this.showResult(`GET error: ${err.message}`, 'error');
            }
            this.opLoading = false;
        },

        async doPut() {
            if (!this.opKey || !this.opValue) {
                this.showResult('Please enter key and value', 'error');
                return;
            }

            this.opLoading = true;
            try {
                const body = {
                    key: this.opKey,
                    value: this.opValue,
                };
                if (this.opTtl) {
                    body.ttl_seconds = parseInt(this.opTtl, 10);
                }

                const res = await fetch('/api/cache/put', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(body),
                });
                const data = await res.json();

                if (data.success) {
                    const shardInfo = data.shard_id !== undefined ? ` (shard ${data.shard_id})` : '';
                    const latency = data.latency_ms ? `, ${data.latency_ms.toFixed(2)}ms` : '';
                    const ttlInfo = this.opTtl ? `, TTL ${this.opTtl}s` : '';
                    this.showResult(`PUT "${this.opKey}" = "${this.opValue}"${ttlInfo}${shardInfo}${latency}`, 'success');
                } else {
                    this.showResult(`PUT failed: ${data.error}`, 'error');
                }
            } catch (err) {
                this.showResult(`PUT error: ${err.message}`, 'error');
            }
            this.opLoading = false;
        },

        async doDelete() {
            if (!this.opKey) {
                this.showResult('Please enter a key', 'error');
                return;
            }

            this.opLoading = true;
            try {
                const res = await fetch(`/api/cache/delete/${encodeURIComponent(this.opKey)}`, {
                    method: 'DELETE',
                });
                const data = await res.json();

                if (data.success) {
                    const shardInfo = data.shard_id !== undefined ? ` (shard ${data.shard_id})` : '';
                    const latency = data.latency_ms ? `, ${data.latency_ms.toFixed(2)}ms` : '';
                    this.showResult(`DELETE "${this.opKey}"${shardInfo}${latency}`, 'success');
                } else {
                    this.showResult(`DELETE failed: ${data.error}`, 'error');
                }
            } catch (err) {
                this.showResult(`DELETE error: ${err.message}`, 'error');
            }
            this.opLoading = false;
        },

        clearForm() {
            this.opKey = '';
            this.opValue = '';
            this.opTtl = '';
            this.opResult = null;
        },

        showResult(message, type) {
            this.opResult = message;
            this.opResultType = type;
        },

        // Shard management functions
        async addShard() {
            if (this.shardOperationLoading) return;

            this.shardOperationLoading = true;
            try {
                const res = await fetch('/api/multiraft/shards/add', { method: 'POST' });
                const data = await res.json();

                if (data.success) {
                    this.showResult(
                        `Added shard S${data.shard_id} with ${data.slots_assigned} slots (epoch ${data.new_epoch})`,
                        'success'
                    );
                    // Refresh data
                    await this.fetchSlotStatus();
                    await this.fetchInitialData();
                } else {
                    this.showResult(`Failed to add shard: ${data.error}`, 'error');
                }
            } catch (err) {
                this.showResult(`Error adding shard: ${err.message}`, 'error');
            }
            this.shardOperationLoading = false;
        },

        showRemoveShardDialog() {
            this.removeShardTarget = null;
            this.showRemoveDialog = true;
        },

        async removeShard() {
            if (this.shardOperationLoading || this.removeShardTarget === null) return;

            this.shardOperationLoading = true;
            this.showRemoveDialog = false;

            try {
                const res = await fetch(`/api/multiraft/shards/${this.removeShardTarget}`, {
                    method: 'DELETE'
                });
                const data = await res.json();

                if (data.success) {
                    this.showResult(
                        `Removing shard S${data.shard_id}, redistributing ${data.slots_redistributed} slots (epoch ${data.new_epoch})`,
                        'success'
                    );
                    // Refresh data
                    await this.fetchSlotStatus();
                    await this.fetchInitialData();
                } else {
                    this.showResult(`Failed to remove shard: ${data.error}`, 'error');
                }
            } catch (err) {
                this.showResult(`Error removing shard: ${err.message}`, 'error');
            }

            this.removeShardTarget = null;
            this.shardOperationLoading = false;
        },

        // Formatting helpers
        formatNumber(n) {
            if (n === null || n === undefined) return '0';
            if (n >= 1000000) return (n / 1000000).toFixed(1) + 'M';
            if (n >= 1000) return (n / 1000).toFixed(1) + 'K';
            return n.toString();
        },

        formatPercent(n) {
            if (n === null || n === undefined) return '0%';
            return (n * 100).toFixed(1) + '%';
        },

        formatLatency(ms) {
            if (ms === null || ms === undefined || isNaN(ms)) return '-';
            if (ms < 0.01) return '<0.01ms';
            if (ms < 1) return ms.toFixed(2) + 'ms';
            return ms.toFixed(1) + 'ms';
        },

        formatOpsPerSec(ops) {
            if (ops === null || ops === undefined || isNaN(ops)) return '0';
            if (ops >= 1000000) return (ops / 1000000).toFixed(1) + 'M';
            if (ops >= 1000) return (ops / 1000).toFixed(1) + 'K';
            return ops.toFixed(0);
        },

        // Shard helpers
        getShardClass(shard) {
            const leaderId = shard.leader_id || 0;
            const nodeClass = `shard-node-${(leaderId % 5) + 1}`;
            const localClass = shard.is_leader ? 'shard-local' : '';
            const migratingClass = (shard.incoming_slots > 0 || shard.outgoing_slots > 0) ? 'shard-migrating' : '';
            return `${nodeClass} ${localClass} ${migratingClass}`.trim();
        },

        getLeaderDisplay(shard) {
            if (shard.leader_id === null || shard.leader_id === undefined) {
                return 'N/A';
            }
            return `N${shard.leader_id}`;
        },

        // Cleanup
        destroy() {
            if (this.eventSource) {
                this.eventSource.close();
            }
        }
    }));
});
