package com.ethan.voxyworldgenv2.network;

import com.ethan.voxyworldgenv2.VoxyWorldGenV2;
import com.ethan.voxyworldgenv2.core.Config;
import com.ethan.voxyworldgenv2.integration.ClientVoxyDirectIngester;
import net.fabricmc.fabric.api.client.networking.v1.ClientPlayNetworking;
import net.minecraft.client.Minecraft;
import net.minecraft.client.multiplayer.ClientLevel;
import net.minecraft.resources.ResourceKey;
import net.minecraft.world.level.chunk.DataLayer;
import net.minecraft.world.level.chunk.LevelChunkSection;
import net.minecraft.world.level.block.state.BlockState;
import net.minecraft.world.level.chunk.PalettedContainer;
import net.minecraft.world.level.biome.Biome;
import net.minecraft.core.Holder;
import net.minecraft.core.registries.Registries;
import net.minecraft.network.FriendlyByteBuf;
import net.minecraft.world.level.Level;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class NetworkClientHandler {
    private static final long DEBUG_LOG_INTERVAL_MS = 5_000L;
    private static final Map<ResourceKey<Level>, ArrayDeque<DeferredPayload>> deferredPayloads = new ConcurrentHashMap<>();
    private static final Set<ResourceKey<Level>> readyDimensionsSent = new HashSet<>();
    private static final Map<ResourceKey<Level>, KnownRegionCursor> knownRegionCursors = new ConcurrentHashMap<>();
    private static final Map<ChunkSyncKey, Long> acceptedSectionMasks = new ConcurrentHashMap<>();
    private static long deferredBytes = 0L;

    private record DeferredPayload(NetworkHandler.LODDataPayload payload, long bytes, long queuedAtMs) {}
    private record ChunkSyncKey(ResourceKey<Level> dimension, long chunkPos) {}

    private static final class KnownRegionCursor {
        private final ResourceKey<Level> dimension;
        private final int centerRegionX;
        private final int centerRegionZ;
        private final int radius;
        private int dx;
        private int dz;
        private boolean done;

        private KnownRegionCursor(ResourceKey<Level> dimension, int centerRegionX, int centerRegionZ, int radius) {
            this.dimension = dimension;
            this.centerRegionX = centerRegionX;
            this.centerRegionZ = centerRegionZ;
            this.radius = radius;
            this.dx = -radius;
            this.dz = -radius;
        }

        private List<NetworkHandler.KnownLODRegionsPayload.RegionData> nextBatch(int maxRegions) {
            List<NetworkHandler.KnownLODRegionsPayload.RegionData> batch = new ArrayList<>();
            while (!done && batch.size() < maxRegions) {
                int regionX = centerRegionX + dx;
                int regionZ = centerRegionZ + dz;
                long[] mask = ClientReceivedLodIndex.getRegionMask(dimension, regionX, regionZ);
                if (mask != null) {
                    batch.add(new NetworkHandler.KnownLODRegionsPayload.RegionData(regionX, regionZ, mask));
                }
                advance();
            }
            return batch;
        }

        private void advance() {
            dz++;
            if (dz > radius) {
                dz = -radius;
                dx++;
            }
            if (dx > radius) {
                done = true;
            }
        }
    }

    private static final AtomicLong debugPayloads = new AtomicLong();
    private static final AtomicLong debugSections = new AtomicLong();
    private static final AtomicLong debugBytes = new AtomicLong();
    private static final AtomicLong debugAirBefore = new AtomicLong();
    private static final AtomicLong debugAirAfter = new AtomicLong();
    private static final AtomicLong debugIngestAttempts = new AtomicLong();
    private static final AtomicLong debugIngestAccepted = new AtomicLong();
    private static final AtomicLong debugIngestRejected = new AtomicLong();
    private static final AtomicLong debugDirectAttempts = new AtomicLong();
    private static final AtomicLong debugDirectSuccess = new AtomicLong();
    private static final AtomicLong debugDirectFail = new AtomicLong();
    private static final AtomicLong debugRendererMissing = new AtomicLong();
    private static final AtomicLong debugRawFallback = new AtomicLong();
    private static final AtomicLong debugDeferredQueued = new AtomicLong();
    private static final AtomicLong debugDeferredFlushed = new AtomicLong();
    private static final AtomicLong debugDeferredDropped = new AtomicLong();
    private static final AtomicLong debugAcksSent = new AtomicLong();
    private static final AtomicLong debugReadySent = new AtomicLong();
    private static final AtomicLong debugKnownRegionsSent = new AtomicLong();
    private static final AtomicLong debugKnownChunksAdvertised = new AtomicLong();
    private static final AtomicLong debugIndexWrites = new AtomicLong();
    private static volatile long nextDebugLogAtMs = 0L;
    
    public static void init() {
        ClientPlayNetworking.registerGlobalReceiver(NetworkHandler.HANDSHAKE_ID, (client, networkHandler, buf, responseSender) -> {
            try {
                NetworkHandler.HandshakePayload payload = new NetworkHandler.HandshakePayload(buf);
                client.execute(() -> {
                    boolean connected = payload.serverHasMod() && payload.protocolVersion() == NetworkHandler.PROTOCOL_VERSION;
                    if (payload.serverHasMod() && payload.protocolVersion() != NetworkHandler.PROTOCOL_VERSION) {
                        VoxyWorldGenV2.LOGGER.warn("voxy sync protocol mismatch: client={}, server={}", NetworkHandler.PROTOCOL_VERSION, payload.protocolVersion());
                    }
                    NetworkState.setServerConnected(connected);
                    if (connected) {
                        ClientReceivedLodIndex.configureForCurrentServer();
                    }
                });
            } catch (Exception e) {
                VoxyWorldGenV2.LOGGER.error("failed to decode handshake payload", e);
            }
        });

        ClientPlayNetworking.registerGlobalReceiver(NetworkHandler.LOD_DATA_ID, (client, networkHandler, buf, responseSender) -> {
            try {
                var level = client.level;
                if (level == null) return;
                NetworkHandler.LODDataPayload payload = new NetworkHandler.LODDataPayload(buf);
                client.execute(() -> handleLODData(payload));
            } catch (Exception e) {
                VoxyWorldGenV2.LOGGER.error("failed to decode LOD data payload", e);
            }
        });
    }

    private static void handleLODData(NetworkHandler.LODDataPayload payload) {
        ClientLevel level = Minecraft.getInstance().level;
        if (level == null) return;

        long bytes = payloadBytes(payload);
        NetworkState.incrementReceived(bytes);

        if (!level.dimension().equals(payload.dimension()) || !ClientVoxyDirectIngester.isRendererReady(level)) {
            enqueueDeferred(payload, bytes);
            return;
        }

        if (!processPayload(level, payload, bytes, false)) {
            enqueueDeferred(payload, bytes);
        }
    }

    public static void tick(Minecraft client) {
        ClientLevel level = client.level;
        if (level == null) return;

        if (NetworkState.isServerConnected() && ClientVoxyDirectIngester.isRendererReady(level)) {
            ClientReceivedLodIndex.tickFlush();
            if (Boolean.TRUE.equals(Config.DATA.clientKnownIndexEnabled) && !sendKnownRegions(level)) {
                flushDeferred(level);
                return;
            }
            sendClientReady(level.dimension());
            flushDeferred(level);
        }
    }

    public static void clearSessionState() {
        synchronized (NetworkClientHandler.class) {
            deferredPayloads.clear();
            readyDimensionsSent.clear();
            knownRegionCursors.clear();
            acceptedSectionMasks.clear();
            deferredBytes = 0L;
        }
        ClientReceivedLodIndex.flushDirty();
    }

    private static void flushDeferred(ClientLevel level) {
        if (!ClientVoxyDirectIngester.isRendererReady(level)) return;

        ArrayDeque<DeferredPayload> queue = deferredPayloads.get(level.dimension());
        if (queue == null || queue.isEmpty()) return;

        long maxBytes = Config.DATA.clientDeferredFlushBytesPerTick;
        int maxPayloads = Config.DATA.clientDeferredFlushPayloadsPerTick;
        long flushedBytes = 0L;
        int flushedPayloads = 0;

        while (flushedPayloads < maxPayloads && flushedBytes < maxBytes) {
            DeferredPayload deferred;
            synchronized (NetworkClientHandler.class) {
                deferred = queue.pollFirst();
                if (deferred != null) {
                    deferredBytes -= deferred.bytes();
                }
            }

            if (deferred == null) break;
            if (!level.dimension().equals(deferred.payload().dimension())) {
                enqueueDeferred(deferred.payload(), deferred.bytes());
                break;
            }

            if (System.currentTimeMillis() - deferred.queuedAtMs() > 60_000L) {
                debugDeferredDropped.incrementAndGet();
                continue;
            }

            if (!processPayload(level, deferred.payload(), deferred.bytes(), true)) {
                enqueueDeferred(deferred.payload(), deferred.bytes());
                break;
            }

            flushedBytes += deferred.bytes();
            flushedPayloads++;
            debugDeferredFlushed.incrementAndGet();
        }
    }

    private static boolean processPayload(ClientLevel level, NetworkHandler.LODDataPayload payload, long bytes, boolean fromDeferred) {
        if (!level.dimension().equals(payload.dimension())) return false;
        if (!ClientVoxyDirectIngester.isRendererReady(level)) return false;

        long airBeforeCount = 0;
        long airAfterCount = 0;
        long ingestCount = 0;
        long ingestAcceptedCount = 0;
        long ingestRejectedCount = 0;
        long directAttemptCount = 0;
        long directSuccessCount = 0;
        long directFailCount = 0;
        long rendererMissingCount = 0;
        long rawFallbackCount = 0;
        long acceptedMask = 0L;

        for (NetworkHandler.LODDataPayload.SectionData sectionData : payload.sections()) {
            ByteBuf statesRaw = Unpooled.wrappedBuffer(sectionData.states());
            ByteBuf biomesRaw = Unpooled.wrappedBuffer(sectionData.biomes());
            try {
                // recreate section
                LevelChunkSection section = new LevelChunkSection(level.registryAccess().registryOrThrow(Registries.BIOME));
                
                // we need to read the states and biomes back using RegistryFriendlyByteBuf for palette consistency
                FriendlyByteBuf statesBuf = new FriendlyByteBuf(statesRaw);
                ((PalettedContainer<BlockState>) section.getStates()).read(statesBuf);
                
                FriendlyByteBuf biomesBuf = new FriendlyByteBuf(biomesRaw);
                ((PalettedContainer<Holder<Biome>>) section.getBiomes()).read(biomesBuf);

                boolean airBefore = section.hasOnlyAir();
                section.recalcBlockCounts();
                boolean airAfter = section.hasOnlyAir();
                if (airBefore) airBeforeCount++;
                if (airAfter) airAfterCount++;
                
                // ingest into voxy
                DataLayer bl = sectionData.blockLight() != null ? new DataLayer(sectionData.blockLight()) : null;
                DataLayer sl = sectionData.skyLight() != null ? new DataLayer(sectionData.skyLight()) : null;
                
                directAttemptCount++;
                ClientVoxyDirectIngester.Result directResult = ClientVoxyDirectIngester.directIngest(level, section, payload.pos().x, sectionData.y(), payload.pos().z, bl, sl);
                boolean accepted = directResult == ClientVoxyDirectIngester.Result.SUCCESS;
                if (accepted) {
                    directSuccessCount++;
                } else if (directResult == ClientVoxyDirectIngester.Result.RENDERER_MISSING) {
                    rendererMissingCount++;
                } else {
                    directFailCount++;
                }

                ingestCount++;
                if (accepted) {
                    ingestAcceptedCount++;
                    int bit = sectionData.y() - payload.minY();
                    if (bit >= 0 && bit < Long.SIZE) {
                        acceptedMask |= 1L << bit;
                    }
                } else {
                    ingestRejectedCount++;
                }
                
            } catch (Exception e) {
                VoxyWorldGenV2.LOGGER.error("failed to handle LOD data for chunk " + payload.pos(), e);
            } finally {
                statesRaw.release();
                biomesRaw.release();
            }
        }

        recordDebugPayload(payload, bytes, airBeforeCount, airAfterCount, ingestCount, ingestAcceptedCount, ingestRejectedCount, directAttemptCount, directSuccessCount, directFailCount, rendererMissingCount, rawFallbackCount);
        if (rendererMissingCount > 0) return false;
        if (acceptedMask != 0L) markAcceptedAndMaybeAck(payload, acceptedMask);
        return true;
    }

    private static void markAcceptedAndMaybeAck(NetworkHandler.LODDataPayload payload, long acceptedMask) {
        ChunkSyncKey key = new ChunkSyncKey(payload.dimension(), payload.pos().toLong());
        long mergedMask = acceptedSectionMasks.merge(key, acceptedMask, (existing, update) -> existing | update);
        long fullMask = payload.fullSectionMask() != 0L ? payload.fullSectionMask() : payload.batchSectionMask();
        if ((mergedMask & fullMask) != fullMask) return;

        acceptedSectionMasks.remove(key);
        sendAck(payload);
        ClientReceivedLodIndex.markReceived(payload.dimension(), payload.pos());
        debugIndexWrites.incrementAndGet();
    }

    private static void enqueueDeferred(NetworkHandler.LODDataPayload payload, long bytes) {
        synchronized (NetworkClientHandler.class) {
            ArrayDeque<DeferredPayload> queue = deferredPayloads.computeIfAbsent(payload.dimension(), ignored -> new ArrayDeque<>());
            while ((!queue.isEmpty() && queue.size() >= Config.DATA.clientDeferredMaxPayloads) || deferredBytes + bytes > Config.DATA.clientDeferredMaxBytes) {
                DeferredPayload dropped = queue.pollFirst();
                if (dropped == null) break;
                deferredBytes -= dropped.bytes();
                debugDeferredDropped.incrementAndGet();
            }

            if (queue.size() >= Config.DATA.clientDeferredMaxPayloads || deferredBytes + bytes > Config.DATA.clientDeferredMaxBytes) {
                debugDeferredDropped.incrementAndGet();
                return;
            }

            queue.addLast(new DeferredPayload(payload, bytes, System.currentTimeMillis()));
            deferredBytes += bytes;
            debugDeferredQueued.incrementAndGet();
        }
    }

    private static long payloadBytes(NetworkHandler.LODDataPayload payload) {
        long bytes = 0L;
        for (NetworkHandler.LODDataPayload.SectionData sd : payload.sections()) {
            bytes += sd.states().length;
            bytes += sd.biomes().length;
            if (sd.blockLight() != null) bytes += sd.blockLight().length;
            if (sd.skyLight() != null) bytes += sd.skyLight().length;
        }
        return bytes;
    }

    private static void sendClientReady(ResourceKey<Level> dimension) {
        synchronized (NetworkClientHandler.class) {
            if (!readyDimensionsSent.add(dimension)) return;
        }

        ByteBuf outRaw = Unpooled.buffer();
        try {
            FriendlyByteBuf outBuf = new FriendlyByteBuf(outRaw);
            new NetworkHandler.ClientReadyPayload(dimension).write(outBuf);
            ClientPlayNetworking.send(NetworkHandler.CLIENT_READY_ID, new FriendlyByteBuf(outRaw.retainedDuplicate()));
            debugReadySent.incrementAndGet();
        } finally {
            outRaw.release();
        }
    }

    private static boolean sendKnownRegions(ClientLevel level) {
        if (Minecraft.getInstance().player == null) return true;

        int radius = Config.DATA.clientKnownRegionRadius > 0 ? Config.DATA.clientKnownRegionRadius : 10;
        int perTick = Config.DATA.clientKnownRegionsPerTick > 0 ? Config.DATA.clientKnownRegionsPerTick : 16;
        int centerRegionX = Math.floorDiv(Minecraft.getInstance().player.chunkPosition().x, 32);
        int centerRegionZ = Math.floorDiv(Minecraft.getInstance().player.chunkPosition().z, 32);

        KnownRegionCursor cursor = knownRegionCursors.computeIfAbsent(level.dimension(), ignored -> new KnownRegionCursor(level.dimension(), centerRegionX, centerRegionZ, radius));
        if (cursor.done) return true;

        List<NetworkHandler.KnownLODRegionsPayload.RegionData> batch = cursor.nextBatch(perTick);
        if (!batch.isEmpty()) {
            sendKnownRegionsPayload(level.dimension(), batch);
        }
        return cursor.done;
    }

    private static void sendKnownRegionsPayload(ResourceKey<Level> dimension, List<NetworkHandler.KnownLODRegionsPayload.RegionData> regions) {
        ByteBuf outRaw = Unpooled.buffer();
        try {
            FriendlyByteBuf outBuf = new FriendlyByteBuf(outRaw);
            new NetworkHandler.KnownLODRegionsPayload(dimension, regions).write(outBuf);
            ClientPlayNetworking.send(NetworkHandler.KNOWN_LOD_REGIONS_ID, new FriendlyByteBuf(outRaw.retainedDuplicate()));
            debugKnownRegionsSent.addAndGet(regions.size());
            debugKnownChunksAdvertised.addAndGet(countKnownChunks(regions));
        } finally {
            outRaw.release();
        }
    }

    private static long countKnownChunks(List<NetworkHandler.KnownLODRegionsPayload.RegionData> regions) {
        long count = 0L;
        for (NetworkHandler.KnownLODRegionsPayload.RegionData region : regions) {
            for (long word : region.maskWords()) {
                count += Long.bitCount(word);
            }
        }
        return count;
    }

    private static void sendAck(NetworkHandler.LODDataPayload payload) {
        ByteBuf outRaw = Unpooled.buffer();
        try {
            FriendlyByteBuf outBuf = new FriendlyByteBuf(outRaw);
            new NetworkHandler.LODAckPayload(payload.dimension(), payload.pos()).write(outBuf);
            ClientPlayNetworking.send(NetworkHandler.LOD_ACK_ID, new FriendlyByteBuf(outRaw.retainedDuplicate()));
            debugAcksSent.incrementAndGet();
        } finally {
            outRaw.release();
        }
    }

    private static void recordDebugPayload(NetworkHandler.LODDataPayload payload, long bytes, long airBefore, long airAfter, long ingestAttempts, long ingestAccepted, long ingestRejected, long directAttempts, long directSuccess, long directFail, long rendererMissing, long rawFallback) {
        if (!Config.DATA.debugSync) return;

        debugPayloads.incrementAndGet();
        debugSections.addAndGet(payload.sections().size());
        debugBytes.addAndGet(bytes);
        debugAirBefore.addAndGet(airBefore);
        debugAirAfter.addAndGet(airAfter);
        debugIngestAttempts.addAndGet(ingestAttempts);
        debugIngestAccepted.addAndGet(ingestAccepted);
        debugIngestRejected.addAndGet(ingestRejected);
        debugDirectAttempts.addAndGet(directAttempts);
        debugDirectSuccess.addAndGet(directSuccess);
        debugDirectFail.addAndGet(directFail);
        debugRendererMissing.addAndGet(rendererMissing);
        debugRawFallback.addAndGet(rawFallback);

        long now = System.currentTimeMillis();
        if (now < nextDebugLogAtMs) return;
        synchronized (NetworkClientHandler.class) {
            if (now < nextDebugLogAtMs) return;
            nextDebugLogAtMs = now + DEBUG_LOG_INTERVAL_MS;
            VoxyWorldGenV2.LOGGER.info(
                "voxy sync client: payloads={}, sections={}, bytes={}, airBefore={}, airAfter={}, ingest={}, accepted={}, rejected={}, directAttempt={}, directSuccess={}, directFail={}, rendererMissing={}, rawFallback={}, deferredQueued={}, deferredFlushed={}, deferredDropped={}, queueBytes={}, readySent={}, acksSent={}, knownRegionsSent={}, knownChunksAdvertised={}, indexWrites={}, dim={}, lastChunk={}",
                debugPayloads.getAndSet(0),
                debugSections.getAndSet(0),
                debugBytes.getAndSet(0),
                debugAirBefore.getAndSet(0),
                debugAirAfter.getAndSet(0),
                debugIngestAttempts.getAndSet(0),
                debugIngestAccepted.getAndSet(0),
                debugIngestRejected.getAndSet(0),
                debugDirectAttempts.getAndSet(0),
                debugDirectSuccess.getAndSet(0),
                debugDirectFail.getAndSet(0),
                debugRendererMissing.getAndSet(0),
                debugRawFallback.getAndSet(0),
                debugDeferredQueued.getAndSet(0),
                debugDeferredFlushed.getAndSet(0),
                debugDeferredDropped.getAndSet(0),
                deferredBytes,
                debugReadySent.getAndSet(0),
                debugAcksSent.getAndSet(0),
                debugKnownRegionsSent.getAndSet(0),
                debugKnownChunksAdvertised.getAndSet(0),
                debugIndexWrites.getAndSet(0),
                payload.dimension().location(),
                payload.pos()
            );
        }
    }
}
