package com.pinterest.kafka.tieredstorage.uploader;

import org.junit.jupiter.api.Test;

import java.nio.file.NoSuchFileException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestUtils {

    @Test
    public void testIsAssignableFromRecursive() {
        boolean result = Utils.isAssignableFromRecursive(new Throwable(), NoSuchFileException.class);
        assertFalse(result);

        result = Utils.isAssignableFromRecursive(new Throwable(new CompletionException(new TimeoutException("test"))), NoSuchFileException.class);
        assertFalse(result);

        result = Utils.isAssignableFromRecursive(new Throwable(new CompletionException(new NoSuchFileException("test"))), NoSuchFileException.class);
        assertTrue(result);

        result = Utils.isAssignableFromRecursive(new Exception(), Throwable.class);
        assertTrue(result);

        result = Utils.isAssignableFromRecursive(new Throwable(), Exception.class);
        assertFalse(result);
    }
}
