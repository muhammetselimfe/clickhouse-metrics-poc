import { useState, useMemo } from 'react';
import { useQuery } from '@tanstack/react-query';
import { createClient } from '@clickhouse/client-web';
import PageTransition from '../components/PageTransition';
import QueryEditor from '../components/QueryEditor';
import { useClickhouseUrl } from '../hooks/useClickhouseUrl';
import { useSyncStatus } from '../hooks/useSyncStatus';

interface Chain {
    chain_id: number;
    name: string;
}

interface PopularToken {
    token: string;
    holder_count: number;
}

function IndexerDemo() {
    const [selectedChainId, setSelectedChainId] = useState<number>(43114);
    const { url } = useClickhouseUrl();
    const { chains: syncStatusChains } = useSyncStatus();

    const clickhouse = useMemo(() => createClient({
        url,
        username: "anonymous",
    }), [url]);

    const { data: chains, isLoading: loadingChains } = useQuery<Chain[]>({
        queryKey: ['chains', url],
        queryFn: async () => {
            const result = await clickhouse.query({
                query: 'SELECT chain_id, name FROM chain_status FINAL WHERE chain_id != 0',
                format: 'JSONEachRow',
            });
            const data = await result.json<Chain>();
            return data as Chain[];
        },
        staleTime: 5 * 60 * 1000,
    });

    const { data: popularToken, isLoading: loadingToken } = useQuery<PopularToken>({
        queryKey: ['popularToken', selectedChainId, url],
        queryFn: async () => {
            const result = await clickhouse.query({
                query: `
                    SELECT 
                        hex(token) as token,
                        count() as holder_count
                    FROM erc20_balances
                    WHERE chain_id = ${selectedChainId}
                      AND balance > 0
                    GROUP BY token
                    HAVING holder_count >= 20
                    ORDER BY holder_count DESC
                    LIMIT 1
                `,
                format: 'JSONEachRow',
            });
            const data = await result.json<PopularToken>();
            return Array.isArray(data) && data.length > 0 ? data[0] : null;
        },
        staleTime: 5 * 60 * 1000,
        enabled: !!selectedChainId,
    });

    const holdersQuery = popularToken
        ? `-- Get token holders with pagination (page 2)
-- Token: 0x${popularToken.token}
-- Chain ID: ${selectedChainId}
-- Total holders: ${popularToken.holder_count}
SELECT 
  lower(hex(wallet)) as wallet_address,
  balance,
  last_updated_block
FROM erc20_balances
WHERE chain_id = ${selectedChainId}
  AND token = unhex('${popularToken.token}')
  AND balance > 0
ORDER BY balance DESC
LIMIT 10 OFFSET 10`
        : '-- Loading popular token...';

    return (
        <PageTransition>
            <div className="p-8 space-y-6">
                <div>
                    <h1 className="text-3xl font-bold text-gray-900">ERC-20 Balance Indexer</h1>
                    <p className="text-gray-600 mt-2">
                        Query current token balances indexed from blockchain events. Example shows paginated holder list for the most popular token.
                    </p>
                </div>

                {/* Chain Selector */}
                <div className="bg-white rounded-lg shadow p-4">
                    <div className="flex items-center gap-3">
                        <label className="text-sm font-semibold text-gray-700 whitespace-nowrap">Chain:</label>
                        {loadingChains ? (
                            <p className="text-sm text-gray-500">Loading chains...</p>
                        ) : chains ? (
                            <select
                                value={selectedChainId}
                                onChange={(e) => setSelectedChainId(parseInt(e.target.value))}
                                className="px-3 py-2 border border-gray-300 rounded-lg text-sm font-medium text-gray-900 bg-white hover:bg-gray-50 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500 cursor-pointer"
                            >
                                {chains.map((chain) => {
                                    const syncStatus = syncStatusChains?.find(c => c.chain_id === chain.chain_id);
                                    const needsWarning = syncStatus && syncStatus.syncPercentage < 99.9;
                                    return (
                                        <option key={chain.chain_id} value={chain.chain_id}>
                                            {chain.name} (ID: {chain.chain_id})
                                            {needsWarning ? ` - ${syncStatus.syncPercentage.toFixed(1)}% synced` : ''}
                                        </option>
                                    );
                                })}
                            </select>
                        ) : (
                            <p className="text-sm text-red-600">Error loading chains</p>
                        )}
                        {loadingToken && (
                            <span className="text-sm text-gray-500 ml-2">Loading token data...</span>
                        )}
                    </div>
                </div>

                {popularToken && !loadingToken && (
                    <div className="border-t border-gray-200 pt-8">
                        <QueryEditor
                            key={`holders-${selectedChainId}-${popularToken.token}`}
                            initialQuery={holdersQuery}
                            title="Token Holders Query (Paginated)"
                            description="Get holders 11-20 by balance for the most popular token. Change LIMIT/OFFSET for different pages. Optimized with ORDER BY (chain_id, token, wallet) for fast token-centric queries."
                        />
                    </div>
                )}

                {!popularToken && !loadingToken && (
                    <div className="bg-white rounded-lg shadow p-8 text-center">
                        <p className="text-gray-600">No tokens with 20+ holders found on this chain.</p>
                    </div>
                )}
            </div>
        </PageTransition>
    );
}

export default IndexerDemo;

