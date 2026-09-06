#include "Utils.h"

#pragma GCC diagnostic ignored "-Winvalid-offsetof"

namespace {
    auto zigzagDecode(auto x) { return (x >> 1) ^ (-(x & 1)); }
    auto zigzagEncode(auto n) { return (n << 1) ^ (n >> 31); }
} // namespace

class IosFlagSaver {
public:
    explicit IosFlagSaver(std::ostream& _ios) : ios(_ios), f(_ios.flags()) {}
    ~IosFlagSaver() { ios.flags(f); }

    IosFlagSaver(const IosFlagSaver& rhs) = delete;
    IosFlagSaver& operator=(const IosFlagSaver& rhs) = delete;

private:
    std::ostream& ios;
    std::ios::fmtflags f;
};

void hexdump(const void* data, size_t size) {
    IosFlagSaver iosfs(std::cout);
    const unsigned char* bytes = static_cast<const unsigned char*>(data);

    for (size_t i = 0; i < size; ++i) {
        std::cout << std::hex << std::setw(2) << std::setfill('0')
            << static_cast<int>(bytes[i]) << " ";

        if ((i + 1) % 16 == 0) {
            std::cout << std::endl;
        }
    }

    std::cout << std::endl;
}

template <CompactArrayT T> consteval size_t CompactArray<T>::unitSize() {
    if constexpr (std::is_integral_v<T>) {
        return sizeof(T);
    }
    else {
        static_assert(std::same_as<T, std::array<char, 16>>,
            "Unsupported type");
        return std::size(T());
    }
}

template <CompactArrayT T> size_t CompactArray<T>::size() const {
    return sizeof(uint8_t) + CompactArray<T>::unitSize() * values.size();
}

template <CompactArrayT T>
void CompactArray<T>::fromBuffer(const char* buffer, size_t buffer_size) {
    if (buffer_size < sizeof(uint8_t)) {
        throw std::runtime_error("Buffer size is too small");
    }

    uint8_t array_length = ::fromBuffer<uint8_t>(buffer);
    buffer += sizeof(uint8_t);
    buffer_size -= sizeof(uint8_t);

    for (uint8_t i = 0; i < array_length - 1; ++i) {
        if constexpr (std::is_integral_v<T>) {
            values.push_back(::fromBuffer<T>(buffer));
        }
        else {
            static_assert(std::same_as<T, std::array<char, 16>>,
                "Unsupported type");
            T value;
            std::copy_n(buffer, value.size(), value.begin());
            values.push_back(value);
        }

        buffer += unitSize();
        buffer_size -= unitSize();
    }
}

template <CompactArrayT T> std::string CompactArray<T>::toBuffer() const {
    std::string buffer;
    buffer.append(::toBuffer<uint8_t>(values.size() + 1));

    for (const auto& value : values) {
        if constexpr (std::is_integral_v<T>) {
            buffer.append(::toBuffer(value));
        }
        else {
            static_assert(std::same_as<T, std::array<char, 16>>,
                "Unsupported type");
            buffer.append(value.data(), value.size());
        }
    }
    return buffer;
}

template <CompactArrayT T> std::string CompactArray<T>::toString() const {
    std::string res = "CompactArray{Values=";
    for (const auto& value : values) {
        if constexpr (std::is_integral_v<T>) {
            res += std::to_string(value) + ",";
        }
        else {
            static_assert(std::same_as<T, std::array<char, 16>>,
                "Unsupported type");
            res += charArrToHex(value) + ",";
        }
    }
    return res + "}";
}

void VariableInt::fromBuffer(const char* buffer) {
    int shift = 0;

    while (true) {
        uint8_t byte = *buffer;
        buffer++;
        int_size++;
        value |= (int32_t)(byte & 0x7F) << shift;
        if ((byte & 0x80) == 0) {
            break;
        }
        shift += 7;
    }

    value = zigzagDecode(value);
}

std::string VariableInt::toBuffer() const {
    std::string buffer;
    int32_t value = zigzagEncode(value);

    while (value > 0x7F) {
        buffer.push_back((value & 0x7F) | 0x80);
        value >>= 7;
    }

    buffer.push_back(value);
    return buffer;
}

template <size_t N>
NullableString<N> NullableString<N>::fromBuffer(const char* buffer,
    size_t buffer_size) {
    if (buffer_size < LEN_SIZE) {
        throw std::runtime_error("Buffer size is too small");
    }
    NullableString<N> nullable_string;

    lenT length = ::fromBuffer<lenT>(buffer);
    if constexpr (N == 1) {
        length -= 1; /* Topic Name size + 1 */
    }

    if (length == -1) {
        return nullable_string;
    }

    nullable_string.value = std::string(buffer + sizeof(lenT), length);
    return nullable_string;
}

template <size_t N> size_t NullableString<N>::size() const {
    return LEN_SIZE + value.size();
}

template <size_t N> std::string_view NullableString<N>::toString() const {
    return value;
}

template <size_t N> std::string NullableString<N>::toBuffer() const {
    std::string buffer;
    if constexpr (N == 1) {
        buffer.append(::toBuffer<lenT>(value.size() + 1));
    }
    else {
        buffer.append(::toBuffer<lenT>(value.size()));
    }
    buffer.append(value);
    return buffer;
}

std::string TaggedFields::toBuffer() const {
    std::string buffer;
    buffer.append(::toBuffer(fieldCount));
    return buffer;
}

void TaggedFields::fromBuffer(const char* buffer, size_t buffer_size) {
    if (buffer_size < sizeof(fieldCount)) {
        throw std::runtime_error("Buffer size is too small");
    }

    fieldCount = ::fromBuffer<uint8_t>(buffer);
}

std::string TaggedFields::toString() const {
    return "TaggedFields{fieldCount=" + std::to_string(fieldCount) + "}";
}

#pragma diagnostic(pop)

template struct NullableString<1>;
template struct NullableString<2>;
template struct CompactArray<uint32_t>;
template struct CompactArray<std::array<char, 16>>;