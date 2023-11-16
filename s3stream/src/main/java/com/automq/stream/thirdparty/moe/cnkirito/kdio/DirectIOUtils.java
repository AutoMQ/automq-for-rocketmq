/**
 * Copyright 2019 xujingfeng (kirito.moe@foxmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.automq.stream.thirdparty.moe.cnkirito.kdio;

import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import com.sun.jna.ptr.PointerByReference;

import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class DirectIOUtils {
    public static final ByteOrder NATIVE_BYTE_ORDER = ByteOrder.nativeOrder();

    /**
     * Allocate <tt>capacity</tt> bytes of native memory for use as a buffer, and
     * return a {@link ByteBuffer} which gives an interface to this memory. The
     * memory is allocated with
     * {@link DirectIOLib#posix_memalign(PointerByReference, NativeLong, NativeLong) DirectIOLib#posix_memalign()}
     * to ensure that the buffer can be used with <tt>O_DIRECT</tt>.
     **
     * @param capacity The requested number of bytes to allocate
     *
     * @return A new JnaMemAlignedBuffer of <tt>capacity</tt> bytes aligned in native memory.
     */
    public static ByteBuffer allocateForDirectIO(DirectIOLib lib, int capacity) {
        if (capacity % lib.blockSize() > 0) {
            throw new IllegalArgumentException("Capacity (" + capacity + ") must be a multiple"
                + "of the block size (" + lib.blockSize() + ")");
        }
        NativeLong blockSize = new NativeLong(lib.blockSize());
        PointerByReference pointerToPointer = new PointerByReference();

        // align memory for use with O_DIRECT
        DirectIOLib.posix_memalign(pointerToPointer, blockSize, new NativeLong(capacity));
        return wrapPointer(Pointer.nativeValue(pointerToPointer.getValue()), capacity);
    }

    /**
     * @param ptr Pointer to wrap.
     * @param len Memory location length.
     * @return Byte buffer wrapping the given memory.
     */
    public static ByteBuffer wrapPointer(long ptr, int len) {
        try {
            ByteBuffer buf = (ByteBuffer)NEW_DIRECT_BUF_MTD.invoke(JAVA_NIO_ACCESS_OBJ, ptr, len, null);

            assert buf.isDirect();
            return buf;
        }
        catch (ReflectiveOperationException e) {
            throw new RuntimeException("JavaNioAccess#newDirectByteBuffer() method is unavailable.", e);
        }
    }

    /** JavaNioAccess object. */
    private static final Object JAVA_NIO_ACCESS_OBJ = javaNioAccessObject();

    /** JavaNioAccess#newDirectByteBuffer method. */
    private static final Method NEW_DIRECT_BUF_MTD = newDirectBufferMethod();

    /**
     * Returns reference to {@code JavaNioAccess.newDirectByteBuffer} method
     * from private API for corresponding Java version.
     *
     * @return Reference to {@code JavaNioAccess.newDirectByteBuffer} method
     * @throws RuntimeException If getting access to the private API is failed.
     */
    private static Method newDirectBufferMethod() {

        try {
            Class<?> cls = JAVA_NIO_ACCESS_OBJ.getClass();

            Method mtd = cls.getMethod("newDirectByteBuffer", long.class, int.class, Object.class);

            mtd.setAccessible(true);

            return mtd;
        }
        catch (ReflectiveOperationException e) {
            throw new RuntimeException(miscPackage() + ".JavaNioAccess#newDirectByteBuffer() method is unavailable.", e);
        }
    }

    /**
     * Returns {@code JavaNioAccess} instance from private API for corresponding Java version.
     *
     * @return {@code JavaNioAccess} instance for corresponding Java version.
     * @throws RuntimeException If getting access to the private API is failed.
     */
    private static Object javaNioAccessObject() {
        String pkgName = miscPackage();

        try {
            Class<?> cls = Class.forName(pkgName + ".misc.SharedSecrets");

            Method mth = cls.getMethod("getJavaNioAccess");

            return mth.invoke(null);
        }
        catch (ReflectiveOperationException e) {
            throw new RuntimeException(pkgName + ".misc.JavaNioAccess class is unavailable.", e);
        }
    }

    private static String miscPackage() {
        // Need return 'jdk.interna' if current Java version >= 9
        return "sun";
    }
}
