package com.ethan.voxyworldgenv2.core;

import com.ethan.voxyworldgenv2.VoxyWorldGenV2;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import net.fabricmc.loader.api.FabricLoader;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public final class Config {
    
    private static final Path CONFIG_PATH = FabricLoader.getInstance().getConfigDir().resolve("voxyworldgenv2.json");
    private static final Gson GSON = new GsonBuilder().setPrettyPrinting().create();
    
    public static ConfigData DATA = new ConfigData();
    
    public static void load() {
        if (!Files.exists(CONFIG_PATH)) {
            // auto-configure for first run
            int cores = Runtime.getRuntime().availableProcessors();
            long maxMemory = Runtime.getRuntime().maxMemory() / (1024 * 1024); // mb
            
            // scale based on hardware
            DATA.maxActiveTasks = 20;
            DATA.generationRadius = 128; // standard default
            
            save();
            return;
        }
        
        try (var reader = Files.newBufferedReader(CONFIG_PATH)) {
            DATA = GSON.fromJson(reader, ConfigData.class);
            applyDefaults();
        } catch (IOException e) {
            VoxyWorldGenV2.LOGGER.error("failed to load config", e);
        }
    }

    private static void applyDefaults() {
        if (DATA == null) DATA = new ConfigData();
        if (DATA.generationRadius <= 0) DATA.generationRadius = 128;
        if (DATA.maxQueueSize <= 0) DATA.maxQueueSize = 20000;
        if (DATA.maxActiveTasks <= 0) DATA.maxActiveTasks = 20;
        if (DATA.update_interval <= 0) DATA.update_interval = 20;
        if (DATA.syncEnabled == null) DATA.syncEnabled = true;
        if (DATA.syncChunksPerSecond <= 0) DATA.syncChunksPerSecond = 25;
        if (DATA.syncGlobalChunksPerSecond <= 0) DATA.syncGlobalChunksPerSecond = 35;
        if (DATA.syncBytesPerSecond <= 0) DATA.syncBytesPerSecond = 1L * 1024L * 1024L;
        if (DATA.syncMaxDispatchPerLoop <= 0) DATA.syncMaxDispatchPerLoop = 2;
        if (DATA.syncMaxLoadsInFlight <= 0) DATA.syncMaxLoadsInFlight = 4;
        if (DATA.syncMinTps <= 0.0) DATA.syncMinTps = 18.0;
        if (DATA.syncResumeTps <= 0.0) DATA.syncResumeTps = 19.0;
        if (DATA.syncResumeTps < DATA.syncMinTps) DATA.syncResumeTps = DATA.syncMinTps;
        if (DATA.syncThrottleCooldownTicks <= 0) DATA.syncThrottleCooldownTicks = 100;
        if (DATA.syncAckTimeoutTicks <= 0) DATA.syncAckTimeoutTicks = 200;
        if (DATA.clientDeferredMaxBytes <= 0) DATA.clientDeferredMaxBytes = 256L * 1024L * 1024L;
        if (DATA.clientDeferredMaxPayloads <= 0) DATA.clientDeferredMaxPayloads = 10000;
        if (DATA.clientDeferredFlushBytesPerTick <= 0) DATA.clientDeferredFlushBytesPerTick = 2L * 1024L * 1024L;
        if (DATA.clientDeferredFlushPayloadsPerTick <= 0) DATA.clientDeferredFlushPayloadsPerTick = 128;
    }
    
    public static void save() {
        try {
            Files.createDirectories(CONFIG_PATH.getParent());
            try (var writer = Files.newBufferedWriter(CONFIG_PATH)) {
                GSON.toJson(DATA, writer);
            }
        } catch (IOException e) {
            VoxyWorldGenV2.LOGGER.error("failed to save config", e);
        }
    }
    
    public static class ConfigData {
        public boolean enabled = true;
        public boolean showF3MenuStats = true;
        public boolean debugSync = false;
        public int generationRadius = 128;
        public int update_interval = 20; // legacy field for Compat
        public int maxQueueSize = 20000;
        public int maxActiveTasks = 20;
        public Boolean syncEnabled = true;
        public int syncChunksPerSecond = 25;
        public int syncGlobalChunksPerSecond = 35;
        public long syncBytesPerSecond = 1L * 1024L * 1024L;
        public int syncMaxDispatchPerLoop = 2;
        public int syncMaxLoadsInFlight = 4;
        public double syncMinTps = 18.0;
        public double syncResumeTps = 19.0;
        public int syncThrottleCooldownTicks = 100;
        public int syncAckTimeoutTicks = 200;
        public long clientDeferredMaxBytes = 256L * 1024L * 1024L;
        public int clientDeferredMaxPayloads = 10000;
        public long clientDeferredFlushBytesPerTick = 2L * 1024L * 1024L;
        public int clientDeferredFlushPayloadsPerTick = 128;
    }
}
