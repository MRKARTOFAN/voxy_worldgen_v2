package com.ethan.voxyworldgenv2.network;

import com.ethan.voxyworldgenv2.VoxyWorldGenV2;
import com.ethan.voxyworldgenv2.core.Config;
import com.ethan.voxyworldgenv2.core.PlayerTracker;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import net.fabricmc.fabric.api.networking.v1.ServerPlayNetworking;
import net.minecraft.core.SectionPos;
import net.minecraft.core.registries.Registries;
import net.minecraft.network.FriendlyByteBuf;
import net.minecraft.resources.ResourceKey;
import net.minecraft.resources.ResourceLocation;
import net.minecraft.server.level.ServerPlayer;
import net.minecraft.world.level.ChunkPos;
import net.minecraft.world.level.Level;
import net.minecraft.world.level.LightLayer;
import net.minecraft.world.level.chunk.DataLayer;
import net.minecraft.world.level.chunk.LevelChunk;
import net.minecraft.world.level.chunk.LevelChunkSection;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

public class NetworkHandler {
    public static final ResourceLocation HANDSHAKE_ID = Objects.requireNonNull(ResourceLocation.tryBuild(VoxyWorldGenV2.MOD_ID, "handshake"));
    public static final ResourceLocation LOD_DATA_ID = Objects.requireNonNull(ResourceLocation.tryBuild(VoxyWorldGenV2.MOD_ID, "lod_data"));
    public static final ResourceLocation CLIENT_READY_ID = Objects.requireNonNull(ResourceLocation.tryBuild(VoxyWorldGenV2.MOD_ID, "client_ready"));
    public static final ResourceLocation LOD_ACK_ID = Objects.requireNonNull(ResourceLocation.tryBuild(VoxyWorldGenV2.MOD_ID, "lod_ack"));

    // keep individual packets well under Netty's 2MB limit to prevent connection resets on public servers
    private static final int MAX_PACKET_BYTES = 32_768;
    private static final long DEBUG_LOG_INTERVAL_MS = 5_000L;
    private static final AtomicLong debugPayloadsSent = new AtomicLong();
    private static final AtomicLong debugSectionsSent = new AtomicLong();
    private static final AtomicLong debugBytesSent = new AtomicLong();
    private static final AtomicLong debugAcksReceived = new AtomicLong();
    private static final AtomicLong debugReadyReceived = new AtomicLong();
    private static final AtomicLong debugThrottled = new AtomicLong();
    private static final AtomicLong debugNotReady = new AtomicLong();
    private static volatile long nextDebugLogAtMs = 0L;

    public record HandshakePayload(boolean serverHasMod) {
        public HandshakePayload(FriendlyByteBuf buf) {
            this(buf.readBoolean());
        }

        public void write(FriendlyByteBuf buf) {
            buf.writeBoolean(this.serverHasMod);
        }
    }

    public record LODDataPayload(ResourceKey<Level> dimension, ChunkPos pos, int minY, List<SectionData> sections) {

        public record SectionData(int y, byte[] states, byte[] biomes, byte[] blockLight, byte[] skyLight) {
            public void write(FriendlyByteBuf buf) {
                buf.writeInt(y);
                buf.writeByteArray(states);
                buf.writeByteArray(biomes);
                buf.writeNullable(blockLight, FriendlyByteBuf::writeByteArray);
                buf.writeNullable(skyLight, FriendlyByteBuf::writeByteArray);
            }
            public static SectionData read(FriendlyByteBuf buf) {
                return new SectionData(
                    buf.readInt(),
                    buf.readByteArray(),
                    buf.readByteArray(),
                    buf.readNullable(b -> b.readByteArray()),
                    buf.readNullable(b -> b.readByteArray())
                );
            }
        }

        public LODDataPayload(FriendlyByteBuf buf) {
            this(
                ResourceKey.create(Registries.DIMENSION, Objects.requireNonNull(ResourceLocation.tryParse(buf.readUtf()), "invalid dimension resource location in LOD payload")),
                buf.readChunkPos(),
                buf.readInt(),
                buf.readCollection(ArrayList::new, b -> SectionData.read((FriendlyByteBuf) b))
            );
        }

        public void write(FriendlyByteBuf buf) {
            buf.writeUtf(dimension.location().toString());
            buf.writeChunkPos(pos);
            buf.writeInt(minY);
            // cast to avoid ambiguous writeCollection / BiConsumer type issues
            buf.writeCollection(sections, (b, s) -> s.write((FriendlyByteBuf) b));
        }

    }

    public record ClientReadyPayload(ResourceKey<Level> dimension) {
        public ClientReadyPayload(FriendlyByteBuf buf) {
            this(ResourceKey.create(Registries.DIMENSION, Objects.requireNonNull(ResourceLocation.tryParse(buf.readUtf()), "invalid dimension resource location in ready payload")));
        }

        public void write(FriendlyByteBuf buf) {
            buf.writeUtf(dimension.location().toString());
        }
    }

    public record LODAckPayload(ResourceKey<Level> dimension, ChunkPos pos) {
        public LODAckPayload(FriendlyByteBuf buf) {
            this(
                ResourceKey.create(Registries.DIMENSION, Objects.requireNonNull(ResourceLocation.tryParse(buf.readUtf()), "invalid dimension resource location in ack payload")),
                buf.readChunkPos()
            );
        }

        public void write(FriendlyByteBuf buf) {
            buf.writeUtf(dimension.location().toString());
            buf.writeChunkPos(pos);
        }
    }

    public record SendResult(boolean sent, boolean throttled, boolean notReady, long bytes) {
        public static SendResult sent(long bytes) {
            return new SendResult(true, false, false, bytes);
        }

        public static SendResult throttled(long bytes) {
            return new SendResult(false, true, false, bytes);
        }

        public static SendResult notReadyResult() {
            return new SendResult(false, false, true, 0L);
        }

        public static SendResult empty() {
            return new SendResult(false, false, false, 0L);
        }
    }

    public static void init() {
        ServerPlayNetworking.registerGlobalReceiver(CLIENT_READY_ID, (server, player, handler, buf, responseSender) -> {
            try {
                ClientReadyPayload payload = new ClientReadyPayload(buf);
                server.execute(() -> {
                    PlayerTracker.getInstance().markClientReady(player.getUUID(), payload.dimension());
                    debugReadyReceived.incrementAndGet();
                    recordDebugPayload(null, 0);
                });
            } catch (Exception e) {
                VoxyWorldGenV2.LOGGER.error("failed to decode client ready payload", e);
            }
        });

        ServerPlayNetworking.registerGlobalReceiver(LOD_ACK_ID, (server, player, handler, buf, responseSender) -> {
            try {
                LODAckPayload payload = new LODAckPayload(buf);
                server.execute(() -> {
                    PlayerTracker.getInstance().markLodAck(player.getUUID(), payload.dimension(), payload.pos());
                    debugAcksReceived.incrementAndGet();
                    recordDebugPayload(null, 0);
                });
            } catch (Exception e) {
                VoxyWorldGenV2.LOGGER.error("failed to decode LOD ack payload", e);
            }
        });

        VoxyWorldGenV2.LOGGER.info("voxy networking initialized");
    }

    private static void sendLODPayload(ServerPlayer player, LODDataPayload payload) {
        ByteBuf outRaw = Unpooled.buffer();
        try {
            FriendlyByteBuf outBuf = new FriendlyByteBuf(outRaw);
            payload.write(outBuf);
            ServerPlayNetworking.send(player, LOD_DATA_ID, new FriendlyByteBuf(outRaw.retainedDuplicate()));
        } finally {
            outRaw.release();
        }
    }

    public static void broadcastLODData(LevelChunk chunk) {
        ChunkPos pos = chunk.getPos();
        int minY = chunk.getMinSection();
        List<LODDataPayload.SectionData> sections = buildSections(chunk);
        if (sections.isEmpty()) return;

        double maxDistSq = 4096.0 * 4096.0;

        for (ServerPlayer player : PlayerTracker.getInstance().getPlayers()) {
            double dx = player.getX() - (pos.getMiddleBlockX());
            double dz = player.getZ() - (pos.getMiddleBlockZ());

            if (player.level() != chunk.getLevel() || (dx * dx + dz * dz > maxDistSq)) {
                PlayerTracker.getInstance().markUnsynced(player.getUUID(), chunk.getLevel().dimension(), pos);
                continue;
            }

            sendLODData(player, chunk);
        }
    }

    public static SendResult sendLODData(ServerPlayer player, LevelChunk chunk) {
        ChunkPos pos = chunk.getPos();
        int minY = chunk.getMinSection();
        ResourceKey<Level> dimension = chunk.getLevel().dimension();
        PlayerTracker tracker = PlayerTracker.getInstance();

        if (!tracker.isClientReady(player.getUUID(), dimension)) {
            debugNotReady.incrementAndGet();
            recordDebugPayload(null, 0);
            return SendResult.notReadyResult();
        }

        List<LODDataPayload.SectionData> sections = buildSections(chunk);

        if (sections.isEmpty()) {
            tracker.markLodAck(player.getUUID(), dimension, pos);
            return SendResult.empty();
        }

        long bytes = estimatePayloadBytes(sections);
        long currentTick = player.server.getTickCount();
        if (!tracker.tryReserveSyncBytes(player.getUUID(), bytes, currentTick)) {
            debugThrottled.incrementAndGet();
            recordDebugPayload(null, 0);
            return SendResult.throttled(bytes);
        }

        sendSectionsInBatches(player, dimension, pos, minY, sections);
        tracker.markLodSent(player.getUUID(), dimension, pos, currentTick);
        return SendResult.sent(bytes);
    }

    private static List<LODDataPayload.SectionData> buildSections(LevelChunk chunk) {
        ChunkPos pos = chunk.getPos();
        int minY = chunk.getMinSection();
        List<LODDataPayload.SectionData> sections = new ArrayList<>();
        var lightEngine = chunk.getLevel().getLightEngine();

        for (int i = 0; i < chunk.getSections().length; i++) {
            LevelChunkSection section = chunk.getSections()[i];
            if (section == null || section.hasOnlyAir()) continue;

            io.netty.buffer.ByteBuf statesRaw = io.netty.buffer.Unpooled.buffer();
            io.netty.buffer.ByteBuf biomesRaw = io.netty.buffer.Unpooled.buffer();
            byte[] states, biomes;
            try {
                FriendlyByteBuf statesBuf = new FriendlyByteBuf(statesRaw);
                section.getStates().write(statesBuf);
                states = new byte[statesBuf.readableBytes()];
                statesBuf.readBytes(states);

                FriendlyByteBuf biomesBuf = new FriendlyByteBuf(biomesRaw);
                section.getBiomes().write(biomesBuf);
                biomes = new byte[biomesBuf.readableBytes()];
                biomesBuf.readBytes(biomes);
            } finally {
                statesRaw.release();
                biomesRaw.release();
            }

            int sectionY = minY + i;
            SectionPos sectionPos = SectionPos.of(pos, sectionY);
            DataLayer bl = lightEngine.getLayerListener(LightLayer.BLOCK).getDataLayerData(sectionPos);
            DataLayer sl = lightEngine.getLayerListener(LightLayer.SKY).getDataLayerData(sectionPos);

            sections.add(new LODDataPayload.SectionData(
                sectionY,
                states,
                biomes,
                bl != null ? bl.getData().clone() : null,
                sl != null ? sl.getData().clone() : null
            ));
        }

        return sections;
    }

    private static long estimatePayloadBytes(List<LODDataPayload.SectionData> sections) {
        long bytes = 0L;
        for (LODDataPayload.SectionData sd : sections) {
            bytes += sd.states().length + sd.biomes().length;
            if (sd.blockLight() != null) bytes += sd.blockLight().length;
            if (sd.skyLight() != null) bytes += sd.skyLight().length;
            bytes += 64L;
        }
        return bytes;
    }

    private static void sendSectionsInBatches(ServerPlayer player, ResourceKey<Level> dimension, ChunkPos pos, int minY, List<LODDataPayload.SectionData> sections) {
        List<LODDataPayload.SectionData> batch = new ArrayList<>();
        int batchBytes = 0;

        for (LODDataPayload.SectionData sd : sections) {
            int sectionBytes = sd.states().length + sd.biomes().length
                + (sd.blockLight() != null ? sd.blockLight().length : 0)
                + (sd.skyLight() != null ? sd.skyLight().length : 0);

            if (!batch.isEmpty() && batchBytes + sectionBytes > MAX_PACKET_BYTES) {
                sendLODDataPayload(player, new LODDataPayload(dimension, pos, minY, batch));
                batch = new ArrayList<>();
                batchBytes = 0;
            }

            batch.add(sd);
            batchBytes += sectionBytes;
        }

        if (!batch.isEmpty()) {
            sendLODDataPayload(player, new LODDataPayload(dimension, pos, minY, batch));
        }
    }

    private static void sendLODDataPayload(ServerPlayer player, LODDataPayload payload) {
        ByteBuf outRaw = Unpooled.buffer();
        try {
            FriendlyByteBuf outFb = new FriendlyByteBuf(outRaw);
            payload.write(outFb);
            recordDebugPayload(payload, outFb.readableBytes());
            ServerPlayNetworking.send(player, LOD_DATA_ID, new FriendlyByteBuf(outRaw.retainedDuplicate()));
        } finally {
            outRaw.release();
        }
    }

    private static void recordDebugPayload(LODDataPayload payload, int bytes) {
        if (!Config.DATA.debugSync) return;

        if (payload != null) {
            debugPayloadsSent.incrementAndGet();
            debugSectionsSent.addAndGet(payload.sections().size());
            debugBytesSent.addAndGet(bytes);
        }

        long now = System.currentTimeMillis();
        if (now < nextDebugLogAtMs) return;
        synchronized (NetworkHandler.class) {
            if (now < nextDebugLogAtMs) return;
            nextDebugLogAtMs = now + DEBUG_LOG_INTERVAL_MS;
            VoxyWorldGenV2.LOGGER.info(
                "voxy sync network: payloads={}, sections={}, bytes={}, ready={}, acks={}, throttled={}, notReady={}, lastChunk={}",
                debugPayloadsSent.getAndSet(0),
                debugSectionsSent.getAndSet(0),
                debugBytesSent.getAndSet(0),
                debugReadyReceived.getAndSet(0),
                debugAcksReceived.getAndSet(0),
                debugThrottled.getAndSet(0),
                debugNotReady.getAndSet(0),
                payload != null ? payload.pos() : "none"
            );
        }
    }

    public static void sendHandshake(ServerPlayer player) {
        ByteBuf outRaw = Unpooled.buffer();
        try {
            outRaw.writeBoolean(true);
            ServerPlayNetworking.send(player, HANDSHAKE_ID, new FriendlyByteBuf(outRaw.retainedDuplicate()));
        } finally {
            outRaw.release();
        }
    }
}
