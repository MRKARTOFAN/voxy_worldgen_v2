package com.ethan.voxyworldgenv2.core;

import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import it.unimi.dsi.fastutil.longs.LongSets;
import net.minecraft.resources.ResourceKey;
import net.minecraft.server.level.ServerPlayer;
import net.minecraft.world.level.ChunkPos;
import net.minecraft.world.level.Level;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class PlayerTracker {
    private static final PlayerTracker INSTANCE = new PlayerTracker();
    private final Set<ServerPlayer> players;
    private final Map<UUID, Map<ResourceKey<Level>, LongSet>> syncedChunks;
    private final Map<UUID, Map<ResourceKey<Level>, Map<Long, Long>>> inFlightChunks;
    private final Map<UUID, Set<ResourceKey<Level>>> readyDimensions;
    private final Map<UUID, SyncBudget> syncBudgets;
    private final SyncBudget globalSyncBudget;

    private static final class SyncBudget {
        long windowStartTick = Long.MIN_VALUE;
        long bytesSent = 0L;
        int chunksSent = 0;
    }
    
    private PlayerTracker() {
        this.players = ConcurrentHashMap.newKeySet();
        this.syncedChunks = new ConcurrentHashMap<>();
        this.inFlightChunks = new ConcurrentHashMap<>();
        this.readyDimensions = new ConcurrentHashMap<>();
        this.syncBudgets = new ConcurrentHashMap<>();
        this.globalSyncBudget = new SyncBudget();
    }
    
    public static PlayerTracker getInstance() {
        return INSTANCE;
    }
    
    public void addPlayer(ServerPlayer player) {
        players.add(player);
        UUID uuid = player.getUUID();
        syncedChunks.put(uuid, new ConcurrentHashMap<>());
        inFlightChunks.put(uuid, new ConcurrentHashMap<>());
        readyDimensions.put(uuid, ConcurrentHashMap.newKeySet());
        syncBudgets.put(uuid, new SyncBudget());
    }
    
    public void removePlayer(ServerPlayer player) {
        players.remove(player);
        UUID uuid = player.getUUID();
        syncedChunks.remove(uuid);
        inFlightChunks.remove(uuid);
        readyDimensions.remove(uuid);
        syncBudgets.remove(uuid);
    }
    
    public void clear() {
        players.clear();
        syncedChunks.clear();
        inFlightChunks.clear();
        readyDimensions.clear();
        syncBudgets.clear();
        synchronized (globalSyncBudget) {
            globalSyncBudget.windowStartTick = Long.MIN_VALUE;
            globalSyncBudget.bytesSent = 0L;
            globalSyncBudget.chunksSent = 0;
        }
    }
    
    public Collection<ServerPlayer> getPlayers() {
        return Collections.unmodifiableCollection(players);
    }

    public LongSet getSyncedChunks(UUID uuid, ResourceKey<Level> dimension) {
        Map<ResourceKey<Level>, LongSet> byDimension = syncedChunks.get(uuid);
        if (byDimension == null) return null;
        return byDimension.computeIfAbsent(dimension, ignored -> LongSets.synchronize(new LongOpenHashSet()));
    }

    public LongSet getSyncSkipChunks(UUID uuid, ResourceKey<Level> dimension) {
        LongOpenHashSet skip = new LongOpenHashSet();
        LongSet synced = getSyncedChunks(uuid, dimension);
        if (synced != null) {
            synchronized (synced) {
                skip.addAll(synced);
            }
        }

        Map<Long, Long> inFlight = getInFlightChunks(uuid, dimension);
        if (inFlight != null) {
            for (Long pos : inFlight.keySet()) {
                skip.add(pos.longValue());
            }
        }
        return skip;
    }

    public void markClientReady(UUID uuid, ResourceKey<Level> dimension) {
        Set<ResourceKey<Level>> ready = readyDimensions.get(uuid);
        if (ready != null) ready.add(dimension);
    }

    public boolean isClientReady(UUID uuid, ResourceKey<Level> dimension) {
        Set<ResourceKey<Level>> ready = readyDimensions.get(uuid);
        return ready != null && ready.contains(dimension);
    }

    public void markLodSent(UUID uuid, ResourceKey<Level> dimension, ChunkPos pos, long tick) {
        Map<Long, Long> inFlight = getOrCreateInFlightChunks(uuid, dimension);
        if (inFlight != null) inFlight.put(pos.toLong(), tick);
    }

    public void markLodAck(UUID uuid, ResourceKey<Level> dimension, ChunkPos pos) {
        Map<Long, Long> inFlight = getInFlightChunks(uuid, dimension);
        if (inFlight != null) inFlight.remove(pos.toLong());

        LongSet synced = getSyncedChunks(uuid, dimension);
        if (synced != null) synced.add(pos.toLong());
    }

    public void markUnsynced(UUID uuid, ResourceKey<Level> dimension, ChunkPos pos) {
        LongSet synced = getSyncedChunks(uuid, dimension);
        if (synced != null) synced.remove(pos.toLong());

        Map<Long, Long> inFlight = getInFlightChunks(uuid, dimension);
        if (inFlight != null) inFlight.remove(pos.toLong());
    }

    public long expireInFlight(UUID uuid, ResourceKey<Level> dimension, long currentTick, long timeoutTicks) {
        Map<Long, Long> inFlight = getInFlightChunks(uuid, dimension);
        if (inFlight == null || inFlight.isEmpty()) return 0L;

        long expired = 0L;
        for (var entry : inFlight.entrySet()) {
            if (currentTick - entry.getValue() >= timeoutTicks && inFlight.remove(entry.getKey(), entry.getValue())) {
                expired++;
            }
        }
        return expired;
    }

    public boolean tryReserveSyncBytes(UUID uuid, long bytes, long currentTick) {
        long limit = Config.DATA.syncBytesPerSecond > 0 ? Config.DATA.syncBytesPerSecond : 1L * 1024L * 1024L;
        SyncBudget budget = syncBudgets.computeIfAbsent(uuid, ignored -> new SyncBudget());

        synchronized (budget) {
            resetBudgetWindow(budget, currentTick);

            if (budget.bytesSent > 0L && budget.bytesSent + bytes > limit) {
                return false;
            }

            budget.bytesSent += bytes;
            return true;
        }
    }

    public boolean tryReserveSyncChunk(UUID uuid, long currentTick) {
        int perPlayerLimit = Config.DATA.syncChunksPerSecond > 0 ? Config.DATA.syncChunksPerSecond : 25;
        int globalLimit = Config.DATA.syncGlobalChunksPerSecond > 0 ? Config.DATA.syncGlobalChunksPerSecond : 35;
        SyncBudget playerBudget = syncBudgets.computeIfAbsent(uuid, ignored -> new SyncBudget());

        synchronized (globalSyncBudget) {
            resetBudgetWindow(globalSyncBudget, currentTick);
            if (globalSyncBudget.chunksSent >= globalLimit) {
                return false;
            }

            synchronized (playerBudget) {
                resetBudgetWindow(playerBudget, currentTick);
                if (playerBudget.chunksSent >= perPlayerLimit) {
                    return false;
                }

                globalSyncBudget.chunksSent++;
                playerBudget.chunksSent++;
                return true;
            }
        }
    }

    private void resetBudgetWindow(SyncBudget budget, long currentTick) {
        if (budget.windowStartTick == Long.MIN_VALUE || currentTick - budget.windowStartTick >= 20L) {
            budget.windowStartTick = currentTick;
            budget.bytesSent = 0L;
            budget.chunksSent = 0;
        }
    }

    private Map<Long, Long> getInFlightChunks(UUID uuid, ResourceKey<Level> dimension) {
        Map<ResourceKey<Level>, Map<Long, Long>> byDimension = inFlightChunks.get(uuid);
        if (byDimension == null) return null;
        return byDimension.get(dimension);
    }

    private Map<Long, Long> getOrCreateInFlightChunks(UUID uuid, ResourceKey<Level> dimension) {
        Map<ResourceKey<Level>, Map<Long, Long>> byDimension = inFlightChunks.get(uuid);
        if (byDimension == null) return null;
        return byDimension.computeIfAbsent(dimension, ignored -> new ConcurrentHashMap<>());
    }
    
    public int getPlayerCount() {
        return players.size();
    }
}
