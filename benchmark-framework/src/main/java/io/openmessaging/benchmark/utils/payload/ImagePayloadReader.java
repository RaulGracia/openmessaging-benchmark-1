package io.openmessaging.benchmark.utils.payload;


import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ImagePayloadReader implements PayloadReader {
    private static final Logger log = LoggerFactory.getLogger(ImagePayloadReader.class);

    private final int switchAfterBytes;
    private final AtomicLong bytesServedWithCurrentImage = new AtomicLong(0);

    private final List<byte[]> loadedImages = new CopyOnWriteArrayList<>();
    private final Random random = new Random();
    private volatile byte[] currentImage;

    public ImagePayloadReader(int switchAfterMB) {
        this.switchAfterBytes = switchAfterMB * 1024 * 1024;
        log.info("Initialized ImagePayloadReader with switchAfterMB={}", switchAfterMB);
    }

    @Override
    public byte[] load(String directoryPath) {
        if (loadedImages.isEmpty()) {
            loadImages(directoryPath);
            pickNewImage();
        }

        if (bytesServedWithCurrentImage.addAndGet(currentImage.length) >= switchAfterBytes) {
            pickNewImage();
        }

        return currentImage.clone();
    }

    private void loadImages(String directoryPath) {
        File dir = new File(directoryPath);
        if (!dir.exists() || !dir.isDirectory()) {
            throw new PayloadException("Invalid directory: " + directoryPath);
        }

        File[] files = dir.listFiles((d, name) -> name.toLowerCase().matches(".*\\.(jpg|jpeg)"));
        if (files == null || files.length == 0) {
            throw new PayloadException("No .jpg/.jpeg files found in: " + directoryPath);
        }

        for (File file : files) {
            try {
                loadedImages.add(Files.readAllBytes(file.toPath()));
            } catch (IOException e) {
                log.warn("Failed to read image file: {}", file.getName(), e);
            }
        }

        if (loadedImages.isEmpty()) {
            throw new PayloadException("No valid images could be loaded.");
        }

        log.info("Loaded {} images from {}", loadedImages.size(), directoryPath);
    }

    private void pickNewImage() {
        currentImage = loadedImages.get(random.nextInt(loadedImages.size()));
        bytesServedWithCurrentImage.set(currentImage.length); // account for the current payload
        log.info("Switched to new image (size={} bytes)", currentImage.length);
    }
}
