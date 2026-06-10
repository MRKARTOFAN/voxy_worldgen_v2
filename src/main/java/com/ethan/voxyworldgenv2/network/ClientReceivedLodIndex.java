package com.ethan.voxyworldgenv2.network;

import com.ethan.voxyworldgenv2.VoxyWorldGenV2;
import com.ethan.voxyworldgenv2.core.Config;
import net.fabricmc.loader.api.FabricLoader;
import net.minecraft.client.Minecraft;
import net.minecraft.resources.ResourceKey;
import net.minecraft.world.level.ChunkPos;
import net.minecraft.world.level.Level;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public final class ClientReceivedLodIndex {
    private static final int REGION_SIZE = 32;
    private static final int REGION_BITS = REGION_SIZE * REGION_SIZE;
    private static final int WORDS_PER_REGION = REGION_BITS / Long.SIZE;
    private static final long FLUSH_INTERVAL_MS = 5_000L;

    private record RegionKey(String serverKey, String dimension, int regionX, int regionZ) {}

    private static final Map<RegionKey, long[]> regionCache = new ConcurrentHashMap<>();
    private static final Set<RegionKey> dirtyRegions = ConcurrentHashMap.newKeySet();
    private static volatile String serverKey = "UNKNOWN";
    private static volatile long nextFlushAtMs = 0L;

    private ClientReceivedLodIndex() {}

    public static synchronized void configureForCurrentServer() {
        flushDirty();
        regionCache.clear();
        dirtyRegions.clear();
        serverKey = resolveServerKey();
    }

    public static void markReceived(ResourceKey<Level> dimension, ChunkPos pos) {
        if (!Boolean.TRUE.equals(Config.DATA.clientKnownIndexEnabled)) return;

        String dimensionKey = sanitize(dimension.location().toString());
        int regionX = Math.floorDiv(pos.x, REGION_SIZE);
        int regionZ = Math.floorDiv(pos.z, REGION_SIZE);
        int localX = Math.floorMod(pos.x, REGION_SIZE);
        int localZ = Math.floorMod(pos.z, REGION_SIZE);
        int bitIndex = (localZ * REGION_SIZE) + localX;
        int wordIndex = bitIndex / Long.SIZE;
        int bit = bitIndex % Long.SIZE;

        RegionKey key = new RegionKey(serverKey, dimensionKey, regionX, regionZ);
        long[] words = regionCache.computeIfAbsent(key, ClientReceivedLodIndex::loadRegion);
        long before = words[wordIndex];
        words[wordIndex] |= 1L << bit;
        if (before != words[wordIndex]) {
            dirtyRegions.add(key);
        }
    }

    public static long[] getRegionMask(ResourceKey<Level> dimension, int regionX, int regionZ) {
        if (!Boolean.TRUE.equals(Config.DATA.clientKnownIndexEnabled)) return null;

        RegionKey key = new RegionKey(serverKey, sanitize(dimension.location().toString()), regionX, regionZ);
        long[] words = regionCache.computeIfAbsent(key, ClientReceivedLodIndex::loadRegion);
        if (isEmpty(words)) return null;
        return Arrays.copyOf(words, words.length);
    }

    public static void tickFlush() {
        long now = System.currentTimeMillis();
        if (now < nextFlushAtMs) return;
        nextFlushAtMs = now + FLUSH_INTERVAL_MS;
        flushDirty();
    }

    public static synchronized void flushDirty() {
        for (RegionKey key : dirtyRegions.toArray(new RegionKey[0])) {
            long[] words = regionCache.get(key);
            if (words == null) {
                dirtyRegions.remove(key);
                continue;
            }

            try {
                Path path = regionPath(key);
                Files.createDirectories(path.getParent());
                try (DataOutputStream out = new DataOutputStream(Files.newOutputStream(path))) {
                    for (long word : words) {
                        out.writeLong(word);
                    }
                }
                dirtyRegions.remove(key);
            } catch (IOException e) {
                VoxyWorldGenV2.LOGGER.warn("failed to save VoxyWorldGen received LOD index {}", key, e);
            }
        }
    }

    private static long[] loadRegion(RegionKey key) {
        long[] words = new long[WORDS_PER_REGION];
        Path path = regionPath(key);
        if (!Files.exists(path)) return words;

        try (DataInputStream in = new DataInputStream(Files.newInputStream(path))) {
            for (int i = 0; i < words.length; i++) {
                words[i] = in.readLong();
            }
        } catch (IOException e) {
            VoxyWorldGenV2.LOGGER.warn("failed to load VoxyWorldGen received LOD index {}", key, e);
            Arrays.fill(words, 0L);
        }
        return words;
    }

    private static Path regionPath(RegionKey key) {
        return FabricLoader.getInstance().getConfigDir()
            .resolve("voxyworldgenv2")
            .resolve("received")
            .resolve(key.serverKey())
            .resolve(key.dimension())
            .resolve(key.regionX() + "." + key.regionZ() + ".bin");
    }

    private static boolean isEmpty(long[] words) {
        for (long word : words) {
            if (word != 0L) return false;
        }
        return true;
    }

    private static String resolveServerKey() {
        Minecraft mc = Minecraft.getInstance();
        String address = null;
        if (mc.getConnection() != null && mc.getConnection().getServerData() != null) {
            address = mc.getConnection().getServerData().ip;
        } else if (mc.getCurrentServer() != null) {
            address = mc.getCurrentServer().ip;
        }

        if (address == null || address.isBlank()) return "UNKNOWN";
        String normalized = address.contains(":") ? address : address + ":25565";
        return sanitize(normalized.replace(':', '_'));
    }

    private static String sanitize(String value) {
        return value.replaceAll("[^A-Za-z0-9._-]", "_");
    }
}
