package com.ethan.voxyworldgenv2.integration;

import com.ethan.voxyworldgenv2.VoxyWorldGenV2;
import net.minecraft.client.Minecraft;
import net.minecraft.client.multiplayer.ClientLevel;
import net.minecraft.world.level.chunk.DataLayer;
import net.minecraft.world.level.chunk.LevelChunkSection;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

public final class ClientVoxyDirectIngester {
    public enum Result {
        SUCCESS,
        RENDERER_MISSING,
        FAILED
    }

    private static boolean initialized = false;
    private static boolean available = false;
    private static Class<?> lightingSupplierClass;
    private static MethodHandle getRenderSystemHandle;
    private static MethodHandle getEngineHandle;
    private static MethodHandle getMapperHandle;
    private static MethodHandle createEmptySectionHandle;
    private static MethodHandle setPositionHandle;
    private static MethodHandle convertHandle;
    private static MethodHandle mipSectionHandle;
    private static MethodHandle insertUpdateHandle;

    private ClientVoxyDirectIngester() {}

    private static void initialize() {
        if (initialized) return;
        initialized = true;

        try {
            MethodHandles.Lookup lookup = MethodHandles.lookup();
            Class<?> renderSystemAccessorClass = Class.forName("me.cortex.voxy.client.core.IGetVoxyRenderSystem");
            Class<?> voxyRenderSystemClass = Class.forName("me.cortex.voxy.client.core.VoxyRenderSystem");
            Class<?> worldEngineClass = Class.forName("me.cortex.voxy.common.world.WorldEngine");
            Class<?> mapperClass = Class.forName("me.cortex.voxy.common.world.other.Mapper");
            Class<?> voxelizedSectionClass = Class.forName("me.cortex.voxy.common.voxelization.VoxelizedSection");
            Class<?> worldConversionFactoryClass = Class.forName("me.cortex.voxy.common.voxelization.WorldConversionFactory");
            Class<?> worldUpdaterClass = Class.forName("me.cortex.voxy.common.world.WorldUpdater");
            lightingSupplierClass = Class.forName("me.cortex.voxy.common.voxelization.ILightingSupplier");

            getRenderSystemHandle = lookup.unreflect(renderSystemAccessorClass.getMethod("voxy$getRenderSystem"));
            getEngineHandle = lookup.unreflect(voxyRenderSystemClass.getMethod("getEngine"));
            getMapperHandle = lookup.unreflect(worldEngineClass.getMethod("getMapper"));
            createEmptySectionHandle = lookup.unreflect(voxelizedSectionClass.getMethod("createEmpty"));
            setPositionHandle = lookup.unreflect(voxelizedSectionClass.getMethod("setPosition", int.class, int.class, int.class));

            Method convert = worldConversionFactoryClass.getMethod(
                "convert",
                voxelizedSectionClass,
                mapperClass,
                net.minecraft.world.level.chunk.PalettedContainer.class,
                net.minecraft.world.level.chunk.PalettedContainerRO.class,
                lightingSupplierClass
            );
            convertHandle = lookup.unreflect(convert);
            mipSectionHandle = lookup.unreflect(worldConversionFactoryClass.getMethod("mipSection", voxelizedSectionClass, mapperClass));
            insertUpdateHandle = lookup.unreflect(worldUpdaterClass.getMethod("insertUpdate", worldEngineClass, voxelizedSectionClass));

            available = true;
            VoxyWorldGenV2.LOGGER.info("client voxy direct ingester initialized");
        } catch (Exception e) {
            available = false;
            VoxyWorldGenV2.LOGGER.warn("client voxy direct ingester unavailable: {}", e.getMessage());
        }
    }

    public static Result directIngest(ClientLevel level, LevelChunkSection section, int cx, int cy, int cz, DataLayer blockLight, DataLayer skyLight) {
        if (!initialized) initialize();
        if (!available || level == null) return Result.RENDERER_MISSING;

        try {
            Object engine = getEngine();
            if (engine == null) return Result.RENDERER_MISSING;

            Object mapper = getMapperHandle.invoke(engine);
            Object voxelized = createEmptySectionHandle.invoke();
            voxelized = setPositionHandle.invoke(voxelized, cx, cy, cz);
            Object converted = convertHandle.invoke(voxelized, mapper, section.getStates(), section.getBiomes(), createLightingSupplier(blockLight, skyLight));
            mipSectionHandle.invoke(converted, mapper);
            insertUpdateHandle.invoke(engine, converted);
            return Result.SUCCESS;
        } catch (Throwable e) {
            VoxyWorldGenV2.LOGGER.error("failed to direct ingest voxy section", e);
            return Result.FAILED;
        }
    }

    public static boolean isRendererReady(ClientLevel level) {
        if (!initialized) initialize();
        if (!available || level == null) return false;

        try {
            return getEngine() != null;
        } catch (Throwable e) {
            return false;
        }
    }

    private static Object getEngine() throws Throwable {
        Object levelRenderer = Minecraft.getInstance().levelRenderer;
        if (levelRenderer == null) return null;

        Object renderSystem = getRenderSystemHandle.invoke(levelRenderer);
        if (renderSystem == null) return null;

        return getEngineHandle.invoke(renderSystem);
    }

    private static Object createLightingSupplier(DataLayer blockLight, DataLayer skyLight) {
        return Proxy.newProxyInstance(
            lightingSupplierClass.getClassLoader(),
            new Class<?>[] { lightingSupplierClass },
            (proxy, method, args) -> {
                if (!"supply".equals(method.getName())) {
                    return switch (method.getName()) {
                        case "toString" -> "VoxyWorldGenV2LightingSupplier";
                        case "hashCode" -> System.identityHashCode(proxy);
                        case "equals" -> proxy == args[0];
                        default -> null;
                    };
                }

                int x = (Integer) args[0];
                int y = (Integer) args[1];
                int z = (Integer) args[2];
                int block = blockLight != null ? Math.min(15, blockLight.get(x, y, z)) : 0;
                int sky = skyLight != null ? Math.min(15, skyLight.get(x, y, z)) : 0;
                return (byte) (sky | (block << 4));
            }
        );
    }
}
