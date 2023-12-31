package com.myself.big.data.utils;


import com.google.common.math.LongMath;
import org.apache.hadoop.hbase.util.Bytes;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.math.RoundingMode;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;


/**
 * UNSIGNED_*** 开头的主要用作序列化映射到已经存在Hbase表的数据，适配HBase Bytes.toBytes(int、float,long)等方法
 * 时间类型 DATE 测试发现用的是 decodeLong 方法
 * phoenix 与 java数据类型对应关系 https://developer.aliyun.com/article/253048#8
 * phoenix 插入数据是对数据二进制转换 在phoenix-core包中的 org.apache.phoenix.schema.types.PDate org.apache.phoenix.schema.types.PLong 类中
 */
public class PhTypeUtil {
    /**
     *
     */
    private static String dateFormatPattern = "yyyy-MM-dd";
    private static String timeFormatPattern = "HH:mm:ss";
    private static String datetimeFormatPattern = "yyyy-MM-dd HH:mm:ss";
    private static String timestampFormatPattern = "yyyy-MM-dd HH:mm:ss.SSS";

    public static byte[] toBytes(Object v, PhType phType) {
        if (v == null) return null;
        byte[] b = null;
        if (phType == PhType.INTEGER) {
            b = new byte[Bytes.SIZEOF_INT];
            encodeInt(((Number) v).intValue(), b, 0);
        } else if (phType == PhType.UNSIGNED_INT) {
            b = new byte[Bytes.SIZEOF_INT];
            encodeUnsignedInt(((Number) v).intValue(), b, 0);
        } else if (phType == PhType.BIGINT) {
            b = new byte[Bytes.SIZEOF_LONG];
            encodeLong(((Number) v).longValue(), b, 0);
        } else if (phType == PhType.UNSIGNED_BIGINT) {
            b = new byte[Bytes.SIZEOF_LONG];
            encodeUnsignedLong(((Number) v).longValue(), b, 0);
        } else if (phType == PhType.SMAILLINT) {
            b = new byte[Bytes.SIZEOF_SHORT];
            encodeShort(((Number) v).shortValue(), b, 0);
        } else if (phType == PhType.UNSIGNED_SMAILLINT) {
            b = new byte[Bytes.SIZEOF_SHORT];
            encodeUnsignedShort(((Number) v).shortValue(), b, 0);
        } else if (phType == PhType.TINYINT) {
            b = new byte[Bytes.SIZEOF_BYTE];
            encodeByte(((Number) v).byteValue(), b, 0);
        } else if (phType == PhType.UNSIGNED_TINYINT) {
            b = new byte[Bytes.SIZEOF_BYTE];
            encodeUnsignedByte(((Number) v).byteValue(), b, 0);
        } else if (phType == PhType.FLOAT) {
            b = new byte[Bytes.SIZEOF_FLOAT];
            encodeFloat(((Number) v).floatValue(), b, 0);
        } else if (phType == PhType.UNSIGNED_FLOAT) {
            b = new byte[Bytes.SIZEOF_FLOAT];
            encodeUnsignedFloat(((Number) v).floatValue(), b, 0);
        } else if (phType == PhType.DOUBLE) {
            b = new byte[Bytes.SIZEOF_DOUBLE];
            encodeDouble(((Number) v).doubleValue(), b, 0);
        } else if (phType == PhType.UNSIGNED_DOUBLE) {
            b = new byte[Bytes.SIZEOF_DOUBLE];
            encodeUnsignedDouble(((Number) v).doubleValue(), b, 0);
        } else if (phType == PhType.BOOLEAN) {
            if ((Boolean) v) {
                b = new byte[]{1};
            } else {
                b = new byte[]{0};
            }
        } else if (phType == PhType.TIME || phType == PhType.DATE) {
            b = new byte[Bytes.SIZEOF_LONG];
            encodeDate(v, b, 0);
        } else if (phType == PhType.TIMESTAMP) {
            b = new byte[Bytes.SIZEOF_LONG + Bytes.SIZEOF_INT];
            encodeTimestamp(v, b, 0);
        } else if (phType == PhType.VARBINARY) {
            b = (byte[]) v;
        } else if (phType == PhType.VARCHAR || phType == PhType.DEFAULT) {
            b = Bytes.toBytes(v.toString());
        } else if (phType == PhType.DECIMAL) {
            b = encodeDecimal(v);
        }
        return b;
    }

    public static Object toObject(byte[] b, PhType phType) {
        if (b == null) return null;
        Object v = null;
        if (phType == PhType.INTEGER) {
            v = decodeInt(b, 0);
        } else if (phType == PhType.UNSIGNED_INT) {
            v = decodeUnsignedInt(b, 0);
        } else if (phType == PhType.BIGINT) {
            v = decodeLong(b, 0);
        } else if (phType == PhType.UNSIGNED_BIGINT) {
            v = decodeUnsignedLong(b, 0);
        } else if (phType == PhType.SMAILLINT) {
            v = decodeShort(b, 0);
        } else if (phType == PhType.UNSIGNED_SMAILLINT) {
            v = decodeUnsignedShort(b, 0);
        } else if (phType == PhType.TINYINT) {
            v = decodeByte(b, 0);
        } else if (phType == PhType.UNSIGNED_TINYINT) {
            v = decodeUnsignedByte(b, 0);
        } else if (phType == PhType.FLOAT) {
            v = decodeFloat(b, 0);
        } else if (phType == PhType.UNSIGNED_FLOAT) {
            v = decodeUnsignedFloat(b, 0);
        } else if (phType == PhType.DOUBLE) {
            v = decodeDouble(b, 0);
        } else if (phType == PhType.UNSIGNED_DOUBLE) {
            v = decodeUnsignedDouble(b, 0);
        } else if (phType == PhType.BOOLEAN) {
            checkForSufficientLength(b, 0, Bytes.SIZEOF_BOOLEAN);
            if (b[0] == 1) {
                v = true;
            } else if (b[0] == 0) {
                v = false;
            }
        } else if (phType == PhType.TIME || phType == PhType.DATE) {
            v = new Date(decodeLong(b, 0));
        } else if (phType == PhType.TIMESTAMP) {
            long millisDeserialized = decodeLong(b, 0);
            Timestamp ts = new Timestamp(millisDeserialized);
            int nanosDeserialized = decodeInt(b, Bytes.SIZEOF_LONG);
            ts.setNanos(nanosDeserialized < 1000000 ? ts.getNanos() + nanosDeserialized : nanosDeserialized);
            v = ts;
        } else if (phType == PhType.VARBINARY) {
            v = b;
        } else if (phType == PhType.VARCHAR || phType == PhType.DEFAULT) {
            v = Bytes.toString(b);
        } else if (phType == PhType.DECIMAL) {
            v = decodeDecimal(b, 0, b.length);
        }

        return v;
    }


    public static int decodeInt(byte[] bytes, int o) {
        checkForSufficientLength(bytes, o, Bytes.SIZEOF_INT);
        int v;
        v = bytes[o] ^ 0x80; // Flip sign bit back
        for (int i = 1; i < Bytes.SIZEOF_INT; i++) {
            v = (v << 8) + (bytes[o + i] & 0xff);
        }
        return v;
    }

    public static int encodeInt(int v, byte[] b, int o) {
        checkForSufficientLength(b, o, Bytes.SIZEOF_INT);
        b[o + 0] = (byte) ((v >> 24) ^ 0x80); // Flip sign bit so that INTEGER is binary comparable
        b[o + 1] = (byte) (v >> 16);
        b[o + 2] = (byte) (v >> 8);
        b[o + 3] = (byte) v;
        return Bytes.SIZEOF_INT;
    }

    public static int decodeUnsignedInt(byte[] b, int o) {
        checkForSufficientLength(b, o, Bytes.SIZEOF_INT);

        int v = Bytes.toInt(b, o);
        if (v < 0) {
            throw new RuntimeException();
        }
        return v;
    }

    public static int encodeUnsignedInt(int v, byte[] b, int o) {
        checkForSufficientLength(b, o, Bytes.SIZEOF_INT);
        if (v < 0) {
            throw new RuntimeException();
        }
        Bytes.putInt(b, o, v);
        return Bytes.SIZEOF_INT;
    }

    public static long decodeLong(byte[] bytes, int o) {
        checkForSufficientLength(bytes, o, Bytes.SIZEOF_LONG);
        long v;
        byte b = bytes[o];
        v = b ^ 0x80; // Flip sign bit back
        for (int i = 1; i < Bytes.SIZEOF_LONG; i++) {
            b = bytes[o + i];
            v = (v << 8) + (b & 0xff);
        }
        return v;
    }

    public static int encodeLong(long v, byte[] b, int o) {
        // checkForSufficientLength(b, o, Bytes.SIZEOF_LONG);
        b[o + 0] = (byte) ((v >> 56) ^ 0x80); // Flip sign bit so that INTEGER is binary comparable
        b[o + 1] = (byte) (v >> 48);
        b[o + 2] = (byte) (v >> 40);
        b[o + 3] = (byte) (v >> 32);
        b[o + 4] = (byte) (v >> 24);
        b[o + 5] = (byte) (v >> 16);
        b[o + 6] = (byte) (v >> 8);
        b[o + 7] = (byte) v;
        return Bytes.SIZEOF_LONG;
    }

    public static long decodeUnsignedLong(byte[] b, int o) {
        checkForSufficientLength(b, o, Bytes.SIZEOF_LONG);
        long v = 0;
        for (int i = o; i < o + Bytes.SIZEOF_LONG; i++) {
            v <<= 8;
            v ^= b[i] & 0xFF;
        }

        if (v < 0) {
            throw new RuntimeException();
        }
        return v;
    }

    public static int encodeUnsignedLong(long v, byte[] b, int o) {
        checkForSufficientLength(b, o, Bytes.SIZEOF_LONG);
        if (v < 0) {
            throw new RuntimeException();
        }
        Bytes.putLong(b, o, v);
        return Bytes.SIZEOF_LONG;
    }

    public static short decodeShort(byte[] b, int o) {
        checkForSufficientLength(b, o, Bytes.SIZEOF_SHORT);
        int v;
        v = b[o] ^ 0x80; // Flip sign bit back
        for (int i = 1; i < Bytes.SIZEOF_SHORT; i++) {
            v = (v << 8) + (b[o + i] & 0xff);
        }
        return (short) v;
    }

    public static int encodeShort(short v, byte[] b, int o) {
        checkForSufficientLength(b, o, Bytes.SIZEOF_SHORT);
        b[o + 0] = (byte) ((v >> 8) ^ 0x80); // Flip sign bit so that Short is binary comparable
        b[o + 1] = (byte) v;
        return Bytes.SIZEOF_SHORT;
    }

    public static short decodeUnsignedShort(byte[] b, int o) {
        checkForSufficientLength(b, o, Bytes.SIZEOF_SHORT);
        short v = Bytes.toShort(b, o);
        if (v < 0) {
            throw new RuntimeException();
        }
        return v;
    }

    public static int encodeUnsignedShort(short v, byte[] b, int o) {
        checkForSufficientLength(b, o, Bytes.SIZEOF_SHORT);
        if (v < 0) {
            throw new RuntimeException();
        }
        Bytes.putShort(b, o, v);
        return Bytes.SIZEOF_SHORT;
    }

    public static byte decodeByte(byte[] b, int o) {
        checkForSufficientLength(b, o, Bytes.SIZEOF_BYTE);
        int v;
        v = b[o] ^ 0x80; // Flip sign bit back
        return (byte) v;
    }

    public static int encodeByte(byte v, byte[] b, int o) {
        checkForSufficientLength(b, o, Bytes.SIZEOF_BYTE);
        b[o] = (byte) (v ^ 0x80); // Flip sign bit so that Short is binary comparable
        return Bytes.SIZEOF_BYTE;
    }

    public static byte decodeUnsignedByte(byte[] b, int o) {
        checkForSufficientLength(b, o, Bytes.SIZEOF_BYTE);
        byte v = b[o];
        if (v < 0) {
            throw new RuntimeException();
        }
        return v;
    }

    public static int encodeUnsignedByte(byte v, byte[] b, int o) {
        if (v < 0) {
            throw new RuntimeException();
        }
        Bytes.putByte(b, o, v);
        return Bytes.SIZEOF_BYTE;
    }

    public static float decodeFloat(byte[] b, int o) {
        checkForSufficientLength(b, o, Bytes.SIZEOF_INT);
        int value;
        value = Bytes.toInt(b, o);
        value--;
        value ^= (~value >> Integer.SIZE - 1) | Integer.MIN_VALUE;
        return Float.intBitsToFloat(value);
    }


    public static int encodeFloat(float v, byte[] b, int o) {
        checkForSufficientLength(b, o, Bytes.SIZEOF_FLOAT);
        int i = Float.floatToIntBits(v);
        i = (i ^ ((i >> Integer.SIZE - 1) | Integer.MIN_VALUE)) + 1;
        Bytes.putInt(b, o, i);
        return Bytes.SIZEOF_FLOAT;
    }

    public static float decodeUnsignedFloat(byte[] b, int o) {
        checkForSufficientLength(b, o, Bytes.SIZEOF_FLOAT);
        float v = Bytes.toFloat(b, o);
        if (v < 0) {
            throw new RuntimeException();
        }
        return v;
    }

    public static int encodeUnsignedFloat(float v, byte[] b, int o) {
        checkForSufficientLength(b, o, Bytes.SIZEOF_FLOAT);
        if (v < 0) {
            throw new RuntimeException();
        }
        Bytes.putFloat(b, o, v);
        return Bytes.SIZEOF_FLOAT;
    }

    public static double decodeDouble(byte[] bytes, int o) {
        checkForSufficientLength(bytes, o, Bytes.SIZEOF_LONG);
        long l;
        l = Bytes.toLong(bytes, o);
        l--;
        l ^= (~l >> Long.SIZE - 1) | Long.MIN_VALUE;
        return Double.longBitsToDouble(l);
    }


    public static int encodeDouble(double v, byte[] b, int o) {
        checkForSufficientLength(b, o, Bytes.SIZEOF_LONG);
        long l = Double.doubleToLongBits(v);
        l = (l ^ ((l >> Long.SIZE - 1) | Long.MIN_VALUE)) + 1;
        Bytes.putLong(b, o, l);
        return Bytes.SIZEOF_LONG;
    }

    public static double decodeUnsignedDouble(byte[] b, int o) {
        checkForSufficientLength(b, o, Bytes.SIZEOF_DOUBLE);
        double v = Bytes.toDouble(b, o);
        if (v < 0) {
            throw new RuntimeException();
        }
        return v;
    }

    public static int encodeUnsignedDouble(double v, byte[] b, int o) {
        checkForSufficientLength(b, o, Bytes.SIZEOF_DOUBLE);
        if (v < 0) {
            throw new RuntimeException();
        }
        Bytes.putDouble(b, o, v);
        return Bytes.SIZEOF_DOUBLE;
    }

    public static int encodeDate(Object v, byte[] b, int o) {
        if (v instanceof Date) {
            encodeUnsignedLong(((Date) v).getTime(), b, 0);
        } else if (v instanceof String) {
            String dateStr = (String) v;
            int len = dateStr.length();
            Date date = null;
            try {
                if (len == 10 && dateStr.charAt(4) == '-' && dateStr.charAt(7) == '-') {
                    SimpleDateFormat format = new SimpleDateFormat(dateFormatPattern);
                    date = format.parse(dateStr);
                } else if (len == 8 && dateStr.charAt(2) == ':' && dateStr.charAt(5) == ':') {
                    SimpleDateFormat format = new SimpleDateFormat(timeFormatPattern);
                    date = format.parse(dateStr);
                } else if (len == 19 && dateStr.charAt(4) == '-' && dateStr.charAt(7) == '-'
                        && dateStr.charAt(13) == ':' && dateStr.charAt(16) == ':') {
                    SimpleDateFormat format = new SimpleDateFormat(datetimeFormatPattern);
                    date = format.parse(dateStr);
                } else if (len == 23 && dateStr.charAt(4) == '-' && dateStr.charAt(7) == '-'
                        && dateStr.charAt(13) == ':' && dateStr.charAt(16) == ':'
                        && dateStr.charAt(19) == '.') {
                    SimpleDateFormat format = new SimpleDateFormat(timestampFormatPattern);
                    date = format.parse(dateStr);
                }
                if (date != null) {
                    encodeUnsignedLong(date.getTime(), b, 0);
                }
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }
        }
        return Bytes.SIZEOF_LONG;
    }

    public static int encodeTimestamp(Object v, byte[] b, int o) {
        if (v instanceof Timestamp) {
            Timestamp ts = (Timestamp) v;
            encodeUnsignedLong(ts.getTime(), b, o);
            Bytes.putInt(b, Bytes.SIZEOF_LONG, ts.getNanos() % 1000000);
        } else {
            encodeDate(v, b, o);
        }
        return Bytes.SIZEOF_LONG + Bytes.SIZEOF_INT;
    }

    public static byte[] encodeDecimal(Object object) {
        if (object == null) {
            return new byte[0];
        }
        BigDecimal v = (BigDecimal) object;
        v = v.round(DEFAULT_MATH_CONTEXT).stripTrailingZeros();
        int len = getLength(v);
        byte[] result = new byte[Math.min(len, 21)];
        decimalToBytes(v, result, 0, len);
        return result;
    }


    public static BigDecimal decodeDecimal(byte[] bytes, int offset, int length) {
        if (length == 1 && bytes[offset] == ZERO_BYTE) {
            return BigDecimal.ZERO;
        }
        int signum = ((bytes[offset] & 0x80) == 0) ? -1 : 1;
        int scale;
        int index;
        int digitOffset;
        long multiplier = 100L;
        int begIndex = offset + 1;
        if (signum == 1) {
            scale = (byte) (((bytes[offset] & 0x7F) - 65) * -2);
            index = offset + length;
            digitOffset = POS_DIGIT_OFFSET;
        } else {
            scale = (byte) ((~bytes[offset] - 65 - 128) * -2);
            index = offset + length - (bytes[offset + length - 1] == NEG_TERMINAL_BYTE ? 1 : 0);
            digitOffset = -NEG_DIGIT_OFFSET;
        }
        length = index - offset;
        long l = signum * bytes[--index] - digitOffset;
        if (l % 10 == 0) { // trailing zero
            scale--; // drop trailing zero and compensate in the scale
            l /= 10;
            multiplier = 10;
        }
        // Use long arithmetic for as long as we can
        while (index > begIndex) {
            if (l >= MAX_LONG_FOR_DESERIALIZE || multiplier >= Long.MAX_VALUE / 100) {
                multiplier = LongMath.divide(multiplier, 100L, RoundingMode.UNNECESSARY);
                break; // Exit loop early so we don't overflow our multiplier
            }
            int digit100 = signum * bytes[--index] - digitOffset;
            l += digit100 * multiplier;
            multiplier = LongMath.checkedMultiply(multiplier, 100);
        }

        BigInteger bi;
        // If still more digits, switch to BigInteger arithmetic
        if (index > begIndex) {
            bi = BigInteger.valueOf(l);
            BigInteger biMultiplier = BigInteger.valueOf(multiplier).multiply(ONE_HUNDRED);
            do {
                int digit100 = signum * bytes[--index] - digitOffset;
                bi = bi.add(biMultiplier.multiply(BigInteger.valueOf(digit100)));
                biMultiplier = biMultiplier.multiply(ONE_HUNDRED);
            } while (index > begIndex);
            if (signum == -1) {
                bi = bi.negate();
            }
        } else {
            bi = BigInteger.valueOf(l * signum);
        }
        // Update the scale based on the precision
        scale += (length - 2) * 2;
        BigDecimal v = new BigDecimal(bi, scale);
        return v;
    }


    private static int getLength(BigDecimal v) {
        int signum = v.signum();
        if (signum == 0) { // Special case for zero
            return 1;
        }
        return (signum < 0 ? 2 : 1) + (v.precision() + 1 + (v.scale() % 2 == 0 ? 0 : 1)) / 2;
    }

    private static final int MAX_PRECISION = 38;
    private static final MathContext DEFAULT_MATH_CONTEXT = new MathContext(MAX_PRECISION, RoundingMode.HALF_UP);
    private static final Integer MAX_BIG_DECIMAL_BYTES = 21;
    private static final byte ZERO_BYTE = (byte) 0x80;
    private static final byte NEG_TERMINAL_BYTE = (byte) 102;
    private static final int EXP_BYTE_OFFSET = 65;
    private static final int POS_DIGIT_OFFSET = 1;
    private static final int NEG_DIGIT_OFFSET = 101;
    private static final BigInteger MAX_LONG = BigInteger.valueOf(Long.MAX_VALUE);
    private static final BigInteger MIN_LONG = BigInteger.valueOf(Long.MIN_VALUE);
    private static final BigInteger ONE_HUNDRED = BigInteger.valueOf(100);
    private static final long MAX_LONG_FOR_DESERIALIZE = Long.MAX_VALUE / 1000;


    private static int decimalToBytes(BigDecimal v, byte[] result, final int offset, int length) {
        int signum = v.signum();
        if (signum == 0) {
            result[offset] = ZERO_BYTE;
            return 1;
        }
        int index = offset + length;
        int scale = v.scale();
        int expOffset = scale % 2 * (scale < 0 ? -1 : 1);
        int multiplyBy;
        BigInteger divideBy;
        if (expOffset == 0) {
            multiplyBy = 1;
            divideBy = ONE_HUNDRED;
        } else {
            multiplyBy = 10;
            divideBy = BigInteger.TEN;
        }
        // Normalize the scale based on what is necessary to end up with a base 100 decimal (i.e. 10.123e3)
        int digitOffset;
        BigInteger compareAgainst;
        if (signum == 1) {
            digitOffset = POS_DIGIT_OFFSET;
            compareAgainst = MAX_LONG;
            scale -= (length - 2) * 2;
            result[offset] = (byte) ((-(scale + expOffset) / 2 + EXP_BYTE_OFFSET) | 0x80);
        } else {
            digitOffset = NEG_DIGIT_OFFSET;
            compareAgainst = MIN_LONG;
            // Scale adjustment shouldn't include terminal byte in length
            scale -= (length - 2 - 1) * 2;
            result[offset] = (byte) (~(-(scale + expOffset) / 2 + EXP_BYTE_OFFSET + 128) & 0x7F);
            if (length <= MAX_BIG_DECIMAL_BYTES) {
                result[--index] = NEG_TERMINAL_BYTE;
            } else {
                // Adjust length and offset down because we don't have enough room
                length = MAX_BIG_DECIMAL_BYTES;
                index = offset + length;
            }
        }
        BigInteger bi = v.unscaledValue();
        // Use BigDecimal arithmetic until we can fit into a long
        while (bi.compareTo(compareAgainst) * signum > 0) {
            BigInteger[] dandr = bi.divideAndRemainder(divideBy);
            bi = dandr[0];
            int digit = dandr[1].intValue();
            result[--index] = (byte) (digit * multiplyBy + digitOffset);
            multiplyBy = 1;
            divideBy = ONE_HUNDRED;
        }
        long l = bi.longValue();
        do {
            long divBy = 100 / multiplyBy;
            long digit = l % divBy;
            l /= divBy;
            result[--index] = (byte) (digit * multiplyBy + digitOffset);
            multiplyBy = 1;
        } while (l != 0);

        return length;
    }

    private static void checkForSufficientLength(byte[] b, int offset, int requiredLength) {
        if (b.length < offset + requiredLength) {
            throw new RuntimeException
                    ("Expected length of at least " + requiredLength + " bytes, but had " + (b.length - offset));
        }
    }
}
