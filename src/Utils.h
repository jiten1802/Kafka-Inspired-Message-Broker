#pragma once

#include <bits/stdc++.h>
#include <arpa/inet.h> 

#pragma pack(push, 1)

template <typename T>
concept CompactArrayT =
std::integral<T> || std::same_as<T, std::array<char, 16>>;

template <CompactArrayT T> struct CompactArray {
    std::vector<T> values;

    static consteval size_t unitSize();
    size_t size() const;
    void fromBuffer(const char* buffer, size_t buffer_size);
    std::string toBuffer() const;
    std::string toString() const;
};

struct VariableInt {
    int32_t value = 0;
    size_t int_size = 0;

    void fromBuffer(const char* buffer);
    std::string toBuffer() const;
    size_t size() const { return int_size; }
    operator int() const { return value; }
};

template <size_t N> struct NullableString {
    using lenT = std::conditional_t<N == 1, int8_t, int16_t>;
    static_assert(N == 1 || N == 2,
        "NullableString Len only supports 1 or 2 bytes");

    static constexpr size_t LEN_SIZE = sizeof(lenT);
    std::string value = "";

    static NullableString fromBuffer(const char* buffer, size_t buffer_size);
    std::string_view toString() const;
    std::string toBuffer() const;
    size_t size() const;
};

struct TaggedFields {
    uint8_t fieldCount = 0;
    std::string toString() const;

    std::string toBuffer() const;
    void fromBuffer(const char* buffer, size_t buffer_size);
};

#pragma diagnostic(pop)

#pragma GCC diagnostic ignored "-Winvalid-offsetof"

#if __BIG_ENDIAN__
#define htonll(x) (x)
#define ntohll(x) (x)
#else
#define htonll(x) ((((uint64_t)htonl(x & 0xFFFFFFFF)) << 32) + htonl(x >> 32))
#define ntohll(x) ((((uint64_t)ntohl(x & 0xFFFFFFFF)) << 32) + ntohl(x >> 32))
#endif

template <std::integral T> std::string toBuffer(const T& t) {
    std::string buffer;
    buffer.resize(sizeof(T));

    if constexpr (sizeof(T) == 1) {
        *reinterpret_cast<T*>(buffer.data()) = t;
    }
    else if constexpr (sizeof(T) == 2) {
        *reinterpret_cast<T*>(buffer.data()) = htons(t);
    }
    else if constexpr (sizeof(T) == 4) {
        *reinterpret_cast<T*>(buffer.data()) = htonl(t);
    }
    else if constexpr (sizeof(T) == 8) {
        *reinterpret_cast<T*>(buffer.data()) = htonll(t);
    }
    else {
        static_assert(sizeof(T) == 1 || sizeof(T) == 2 || sizeof(T) == 4 ||
            sizeof(T) == 8,
            "Unsupported size");
    }

    return buffer;
}

template <std::integral T> T fromBuffer(const char* buffer) {
    if constexpr (sizeof(T) == 1) {
        return *reinterpret_cast<const T*>(buffer);
    }
    else if constexpr (sizeof(T) == 2) {
        return ntohs(*reinterpret_cast<const T*>(buffer));
    }
    else if constexpr (sizeof(T) == 4) {
        return ntohl(*reinterpret_cast<const T*>(buffer));
    }
    else if constexpr (sizeof(T) == 8) {
        return ntohll(*reinterpret_cast<const T*>(buffer));
    }
    else {
        static_assert(sizeof(T) == 1 || sizeof(T) == 2 || sizeof(T) == 4 ||
            sizeof(T) == 8,
            "Unsupported size");
    }
}

template <size_t N> std::string charArrToHex(std::array<char, N> arr) {
#define HEX(x) std::setw(2) << std::setfill('0') << std::hex << (int)(x)
    std::stringstream ss;
    for (char c : arr)
        ss << HEX(c) << " ";
    std::string str = ss.str();
    return str;
#undef HEX
}

void hexdump(const void* data, size_t size);

#pragma diagnostic(pop)