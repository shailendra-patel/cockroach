// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: cockroach/util/unresolved_addr.proto

#define INTERNAL_SUPPRESS_PROTOBUF_FIELD_DEPRECATION
#include "cockroach/util/unresolved_addr.pb.h"

#include <algorithm>

#include <google/protobuf/stubs/common.h>
#include <google/protobuf/stubs/once.h>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/wire_format_lite_inl.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/generated_message_reflection.h>
#include <google/protobuf/reflection_ops.h>
#include <google/protobuf/wire_format.h>
// @@protoc_insertion_point(includes)

namespace cockroach {
namespace util {

namespace {

const ::google::protobuf::Descriptor* UnresolvedAddr_descriptor_ = NULL;
const ::google::protobuf::internal::GeneratedMessageReflection*
  UnresolvedAddr_reflection_ = NULL;

}  // namespace


void protobuf_AssignDesc_cockroach_2futil_2funresolved_5faddr_2eproto() {
  protobuf_AddDesc_cockroach_2futil_2funresolved_5faddr_2eproto();
  const ::google::protobuf::FileDescriptor* file =
    ::google::protobuf::DescriptorPool::generated_pool()->FindFileByName(
      "cockroach/util/unresolved_addr.proto");
  GOOGLE_CHECK(file != NULL);
  UnresolvedAddr_descriptor_ = file->message_type(0);
  static const int UnresolvedAddr_offsets_[2] = {
    GOOGLE_PROTOBUF_GENERATED_MESSAGE_FIELD_OFFSET(UnresolvedAddr, network_field_),
    GOOGLE_PROTOBUF_GENERATED_MESSAGE_FIELD_OFFSET(UnresolvedAddr, address_field_),
  };
  UnresolvedAddr_reflection_ =
    ::google::protobuf::internal::GeneratedMessageReflection::NewGeneratedMessageReflection(
      UnresolvedAddr_descriptor_,
      UnresolvedAddr::default_instance_,
      UnresolvedAddr_offsets_,
      GOOGLE_PROTOBUF_GENERATED_MESSAGE_FIELD_OFFSET(UnresolvedAddr, _has_bits_[0]),
      -1,
      -1,
      sizeof(UnresolvedAddr),
      GOOGLE_PROTOBUF_GENERATED_MESSAGE_FIELD_OFFSET(UnresolvedAddr, _internal_metadata_),
      -1);
}

namespace {

GOOGLE_PROTOBUF_DECLARE_ONCE(protobuf_AssignDescriptors_once_);
inline void protobuf_AssignDescriptorsOnce() {
  ::google::protobuf::GoogleOnceInit(&protobuf_AssignDescriptors_once_,
                 &protobuf_AssignDesc_cockroach_2futil_2funresolved_5faddr_2eproto);
}

void protobuf_RegisterTypes(const ::std::string&) {
  protobuf_AssignDescriptorsOnce();
  ::google::protobuf::MessageFactory::InternalRegisterGeneratedMessage(
      UnresolvedAddr_descriptor_, &UnresolvedAddr::default_instance());
}

}  // namespace

void protobuf_ShutdownFile_cockroach_2futil_2funresolved_5faddr_2eproto() {
  delete UnresolvedAddr::default_instance_;
  delete UnresolvedAddr_reflection_;
}

void protobuf_AddDesc_cockroach_2futil_2funresolved_5faddr_2eproto() {
  static bool already_here = false;
  if (already_here) return;
  already_here = true;
  GOOGLE_PROTOBUF_VERIFY_VERSION;

  ::gogoproto::protobuf_AddDesc_gogoproto_2fgogo_2eproto();
  ::google::protobuf::DescriptorPool::InternalAddGeneratedFile(
    "\n$cockroach/util/unresolved_addr.proto\022\016"
    "cockroach.util\032\024gogoproto/gogo.proto\"f\n\016"
    "UnresolvedAddr\022&\n\rnetwork_field\030\001 \001(\tB\017\310"
    "\336\037\000\352\336\037\007network\022&\n\raddress_field\030\002 \001(\tB\017\310"
    "\336\037\000\352\336\037\007address:\004\230\240\037\000B\006Z\004utilX\000", 190);
  ::google::protobuf::MessageFactory::InternalRegisterGeneratedFile(
    "cockroach/util/unresolved_addr.proto", &protobuf_RegisterTypes);
  UnresolvedAddr::default_instance_ = new UnresolvedAddr();
  UnresolvedAddr::default_instance_->InitAsDefaultInstance();
  ::google::protobuf::internal::OnShutdown(&protobuf_ShutdownFile_cockroach_2futil_2funresolved_5faddr_2eproto);
}

// Force AddDescriptors() to be called at static initialization time.
struct StaticDescriptorInitializer_cockroach_2futil_2funresolved_5faddr_2eproto {
  StaticDescriptorInitializer_cockroach_2futil_2funresolved_5faddr_2eproto() {
    protobuf_AddDesc_cockroach_2futil_2funresolved_5faddr_2eproto();
  }
} static_descriptor_initializer_cockroach_2futil_2funresolved_5faddr_2eproto_;

namespace {

static void MergeFromFail(int line) GOOGLE_ATTRIBUTE_COLD;
static void MergeFromFail(int line) {
  GOOGLE_CHECK(false) << __FILE__ << ":" << line;
}

}  // namespace


// ===================================================================

#ifndef _MSC_VER
const int UnresolvedAddr::kNetworkFieldFieldNumber;
const int UnresolvedAddr::kAddressFieldFieldNumber;
#endif  // !_MSC_VER

UnresolvedAddr::UnresolvedAddr()
  : ::google::protobuf::Message(), _internal_metadata_(NULL) {
  SharedCtor();
  // @@protoc_insertion_point(constructor:cockroach.util.UnresolvedAddr)
}

void UnresolvedAddr::InitAsDefaultInstance() {
}

UnresolvedAddr::UnresolvedAddr(const UnresolvedAddr& from)
  : ::google::protobuf::Message(),
    _internal_metadata_(NULL) {
  SharedCtor();
  MergeFrom(from);
  // @@protoc_insertion_point(copy_constructor:cockroach.util.UnresolvedAddr)
}

void UnresolvedAddr::SharedCtor() {
  ::google::protobuf::internal::GetEmptyString();
  _cached_size_ = 0;
  network_field_.UnsafeSetDefault(&::google::protobuf::internal::GetEmptyStringAlreadyInited());
  address_field_.UnsafeSetDefault(&::google::protobuf::internal::GetEmptyStringAlreadyInited());
  ::memset(_has_bits_, 0, sizeof(_has_bits_));
}

UnresolvedAddr::~UnresolvedAddr() {
  // @@protoc_insertion_point(destructor:cockroach.util.UnresolvedAddr)
  SharedDtor();
}

void UnresolvedAddr::SharedDtor() {
  network_field_.DestroyNoArena(&::google::protobuf::internal::GetEmptyStringAlreadyInited());
  address_field_.DestroyNoArena(&::google::protobuf::internal::GetEmptyStringAlreadyInited());
  if (this != default_instance_) {
  }
}

void UnresolvedAddr::SetCachedSize(int size) const {
  GOOGLE_SAFE_CONCURRENT_WRITES_BEGIN();
  _cached_size_ = size;
  GOOGLE_SAFE_CONCURRENT_WRITES_END();
}
const ::google::protobuf::Descriptor* UnresolvedAddr::descriptor() {
  protobuf_AssignDescriptorsOnce();
  return UnresolvedAddr_descriptor_;
}

const UnresolvedAddr& UnresolvedAddr::default_instance() {
  if (default_instance_ == NULL) protobuf_AddDesc_cockroach_2futil_2funresolved_5faddr_2eproto();
  return *default_instance_;
}

UnresolvedAddr* UnresolvedAddr::default_instance_ = NULL;

UnresolvedAddr* UnresolvedAddr::New(::google::protobuf::Arena* arena) const {
  UnresolvedAddr* n = new UnresolvedAddr;
  if (arena != NULL) {
    arena->Own(n);
  }
  return n;
}

void UnresolvedAddr::Clear() {
  if (_has_bits_[0 / 32] & 3u) {
    if (has_network_field()) {
      network_field_.ClearToEmptyNoArena(&::google::protobuf::internal::GetEmptyStringAlreadyInited());
    }
    if (has_address_field()) {
      address_field_.ClearToEmptyNoArena(&::google::protobuf::internal::GetEmptyStringAlreadyInited());
    }
  }
  ::memset(_has_bits_, 0, sizeof(_has_bits_));
  if (_internal_metadata_.have_unknown_fields()) {
    mutable_unknown_fields()->Clear();
  }
}

bool UnresolvedAddr::MergePartialFromCodedStream(
    ::google::protobuf::io::CodedInputStream* input) {
#define DO_(EXPRESSION) if (!(EXPRESSION)) goto failure
  ::google::protobuf::uint32 tag;
  // @@protoc_insertion_point(parse_start:cockroach.util.UnresolvedAddr)
  for (;;) {
    ::std::pair< ::google::protobuf::uint32, bool> p = input->ReadTagWithCutoff(127);
    tag = p.first;
    if (!p.second) goto handle_unusual;
    switch (::google::protobuf::internal::WireFormatLite::GetTagFieldNumber(tag)) {
      // optional string network_field = 1;
      case 1: {
        if (tag == 10) {
          DO_(::google::protobuf::internal::WireFormatLite::ReadString(
                input, this->mutable_network_field()));
          ::google::protobuf::internal::WireFormat::VerifyUTF8StringNamedField(
            this->network_field().data(), this->network_field().length(),
            ::google::protobuf::internal::WireFormat::PARSE,
            "cockroach.util.UnresolvedAddr.network_field");
        } else {
          goto handle_unusual;
        }
        if (input->ExpectTag(18)) goto parse_address_field;
        break;
      }

      // optional string address_field = 2;
      case 2: {
        if (tag == 18) {
         parse_address_field:
          DO_(::google::protobuf::internal::WireFormatLite::ReadString(
                input, this->mutable_address_field()));
          ::google::protobuf::internal::WireFormat::VerifyUTF8StringNamedField(
            this->address_field().data(), this->address_field().length(),
            ::google::protobuf::internal::WireFormat::PARSE,
            "cockroach.util.UnresolvedAddr.address_field");
        } else {
          goto handle_unusual;
        }
        if (input->ExpectAtEnd()) goto success;
        break;
      }

      default: {
      handle_unusual:
        if (tag == 0 ||
            ::google::protobuf::internal::WireFormatLite::GetTagWireType(tag) ==
            ::google::protobuf::internal::WireFormatLite::WIRETYPE_END_GROUP) {
          goto success;
        }
        DO_(::google::protobuf::internal::WireFormat::SkipField(
              input, tag, mutable_unknown_fields()));
        break;
      }
    }
  }
success:
  // @@protoc_insertion_point(parse_success:cockroach.util.UnresolvedAddr)
  return true;
failure:
  // @@protoc_insertion_point(parse_failure:cockroach.util.UnresolvedAddr)
  return false;
#undef DO_
}

void UnresolvedAddr::SerializeWithCachedSizes(
    ::google::protobuf::io::CodedOutputStream* output) const {
  // @@protoc_insertion_point(serialize_start:cockroach.util.UnresolvedAddr)
  // optional string network_field = 1;
  if (has_network_field()) {
    ::google::protobuf::internal::WireFormat::VerifyUTF8StringNamedField(
      this->network_field().data(), this->network_field().length(),
      ::google::protobuf::internal::WireFormat::SERIALIZE,
      "cockroach.util.UnresolvedAddr.network_field");
    ::google::protobuf::internal::WireFormatLite::WriteStringMaybeAliased(
      1, this->network_field(), output);
  }

  // optional string address_field = 2;
  if (has_address_field()) {
    ::google::protobuf::internal::WireFormat::VerifyUTF8StringNamedField(
      this->address_field().data(), this->address_field().length(),
      ::google::protobuf::internal::WireFormat::SERIALIZE,
      "cockroach.util.UnresolvedAddr.address_field");
    ::google::protobuf::internal::WireFormatLite::WriteStringMaybeAliased(
      2, this->address_field(), output);
  }

  if (_internal_metadata_.have_unknown_fields()) {
    ::google::protobuf::internal::WireFormat::SerializeUnknownFields(
        unknown_fields(), output);
  }
  // @@protoc_insertion_point(serialize_end:cockroach.util.UnresolvedAddr)
}

::google::protobuf::uint8* UnresolvedAddr::SerializeWithCachedSizesToArray(
    ::google::protobuf::uint8* target) const {
  // @@protoc_insertion_point(serialize_to_array_start:cockroach.util.UnresolvedAddr)
  // optional string network_field = 1;
  if (has_network_field()) {
    ::google::protobuf::internal::WireFormat::VerifyUTF8StringNamedField(
      this->network_field().data(), this->network_field().length(),
      ::google::protobuf::internal::WireFormat::SERIALIZE,
      "cockroach.util.UnresolvedAddr.network_field");
    target =
      ::google::protobuf::internal::WireFormatLite::WriteStringToArray(
        1, this->network_field(), target);
  }

  // optional string address_field = 2;
  if (has_address_field()) {
    ::google::protobuf::internal::WireFormat::VerifyUTF8StringNamedField(
      this->address_field().data(), this->address_field().length(),
      ::google::protobuf::internal::WireFormat::SERIALIZE,
      "cockroach.util.UnresolvedAddr.address_field");
    target =
      ::google::protobuf::internal::WireFormatLite::WriteStringToArray(
        2, this->address_field(), target);
  }

  if (_internal_metadata_.have_unknown_fields()) {
    target = ::google::protobuf::internal::WireFormat::SerializeUnknownFieldsToArray(
        unknown_fields(), target);
  }
  // @@protoc_insertion_point(serialize_to_array_end:cockroach.util.UnresolvedAddr)
  return target;
}

int UnresolvedAddr::ByteSize() const {
  int total_size = 0;

  if (_has_bits_[0 / 32] & 3u) {
    // optional string network_field = 1;
    if (has_network_field()) {
      total_size += 1 +
        ::google::protobuf::internal::WireFormatLite::StringSize(
          this->network_field());
    }

    // optional string address_field = 2;
    if (has_address_field()) {
      total_size += 1 +
        ::google::protobuf::internal::WireFormatLite::StringSize(
          this->address_field());
    }

  }
  if (_internal_metadata_.have_unknown_fields()) {
    total_size +=
      ::google::protobuf::internal::WireFormat::ComputeUnknownFieldsSize(
        unknown_fields());
  }
  GOOGLE_SAFE_CONCURRENT_WRITES_BEGIN();
  _cached_size_ = total_size;
  GOOGLE_SAFE_CONCURRENT_WRITES_END();
  return total_size;
}

void UnresolvedAddr::MergeFrom(const ::google::protobuf::Message& from) {
  if (GOOGLE_PREDICT_FALSE(&from == this)) MergeFromFail(__LINE__);
  const UnresolvedAddr* source = 
      ::google::protobuf::internal::DynamicCastToGenerated<const UnresolvedAddr>(
          &from);
  if (source == NULL) {
    ::google::protobuf::internal::ReflectionOps::Merge(from, this);
  } else {
    MergeFrom(*source);
  }
}

void UnresolvedAddr::MergeFrom(const UnresolvedAddr& from) {
  if (GOOGLE_PREDICT_FALSE(&from == this)) MergeFromFail(__LINE__);
  if (from._has_bits_[0 / 32] & (0xffu << (0 % 32))) {
    if (from.has_network_field()) {
      set_has_network_field();
      network_field_.AssignWithDefault(&::google::protobuf::internal::GetEmptyStringAlreadyInited(), from.network_field_);
    }
    if (from.has_address_field()) {
      set_has_address_field();
      address_field_.AssignWithDefault(&::google::protobuf::internal::GetEmptyStringAlreadyInited(), from.address_field_);
    }
  }
  if (from._internal_metadata_.have_unknown_fields()) {
    mutable_unknown_fields()->MergeFrom(from.unknown_fields());
  }
}

void UnresolvedAddr::CopyFrom(const ::google::protobuf::Message& from) {
  if (&from == this) return;
  Clear();
  MergeFrom(from);
}

void UnresolvedAddr::CopyFrom(const UnresolvedAddr& from) {
  if (&from == this) return;
  Clear();
  MergeFrom(from);
}

bool UnresolvedAddr::IsInitialized() const {

  return true;
}

void UnresolvedAddr::Swap(UnresolvedAddr* other) {
  if (other == this) return;
  InternalSwap(other);
}
void UnresolvedAddr::InternalSwap(UnresolvedAddr* other) {
  network_field_.Swap(&other->network_field_);
  address_field_.Swap(&other->address_field_);
  std::swap(_has_bits_[0], other->_has_bits_[0]);
  _internal_metadata_.Swap(&other->_internal_metadata_);
  std::swap(_cached_size_, other->_cached_size_);
}

::google::protobuf::Metadata UnresolvedAddr::GetMetadata() const {
  protobuf_AssignDescriptorsOnce();
  ::google::protobuf::Metadata metadata;
  metadata.descriptor = UnresolvedAddr_descriptor_;
  metadata.reflection = UnresolvedAddr_reflection_;
  return metadata;
}

#if PROTOBUF_INLINE_NOT_IN_HEADERS
// UnresolvedAddr

// optional string network_field = 1;
bool UnresolvedAddr::has_network_field() const {
  return (_has_bits_[0] & 0x00000001u) != 0;
}
void UnresolvedAddr::set_has_network_field() {
  _has_bits_[0] |= 0x00000001u;
}
void UnresolvedAddr::clear_has_network_field() {
  _has_bits_[0] &= ~0x00000001u;
}
void UnresolvedAddr::clear_network_field() {
  network_field_.ClearToEmptyNoArena(&::google::protobuf::internal::GetEmptyStringAlreadyInited());
  clear_has_network_field();
}
 const ::std::string& UnresolvedAddr::network_field() const {
  // @@protoc_insertion_point(field_get:cockroach.util.UnresolvedAddr.network_field)
  return network_field_.GetNoArena(&::google::protobuf::internal::GetEmptyStringAlreadyInited());
}
 void UnresolvedAddr::set_network_field(const ::std::string& value) {
  set_has_network_field();
  network_field_.SetNoArena(&::google::protobuf::internal::GetEmptyStringAlreadyInited(), value);
  // @@protoc_insertion_point(field_set:cockroach.util.UnresolvedAddr.network_field)
}
 void UnresolvedAddr::set_network_field(const char* value) {
  set_has_network_field();
  network_field_.SetNoArena(&::google::protobuf::internal::GetEmptyStringAlreadyInited(), ::std::string(value));
  // @@protoc_insertion_point(field_set_char:cockroach.util.UnresolvedAddr.network_field)
}
 void UnresolvedAddr::set_network_field(const char* value, size_t size) {
  set_has_network_field();
  network_field_.SetNoArena(&::google::protobuf::internal::GetEmptyStringAlreadyInited(),
      ::std::string(reinterpret_cast<const char*>(value), size));
  // @@protoc_insertion_point(field_set_pointer:cockroach.util.UnresolvedAddr.network_field)
}
 ::std::string* UnresolvedAddr::mutable_network_field() {
  set_has_network_field();
  // @@protoc_insertion_point(field_mutable:cockroach.util.UnresolvedAddr.network_field)
  return network_field_.MutableNoArena(&::google::protobuf::internal::GetEmptyStringAlreadyInited());
}
 ::std::string* UnresolvedAddr::release_network_field() {
  clear_has_network_field();
  return network_field_.ReleaseNoArena(&::google::protobuf::internal::GetEmptyStringAlreadyInited());
}
 void UnresolvedAddr::set_allocated_network_field(::std::string* network_field) {
  if (network_field != NULL) {
    set_has_network_field();
  } else {
    clear_has_network_field();
  }
  network_field_.SetAllocatedNoArena(&::google::protobuf::internal::GetEmptyStringAlreadyInited(), network_field);
  // @@protoc_insertion_point(field_set_allocated:cockroach.util.UnresolvedAddr.network_field)
}

// optional string address_field = 2;
bool UnresolvedAddr::has_address_field() const {
  return (_has_bits_[0] & 0x00000002u) != 0;
}
void UnresolvedAddr::set_has_address_field() {
  _has_bits_[0] |= 0x00000002u;
}
void UnresolvedAddr::clear_has_address_field() {
  _has_bits_[0] &= ~0x00000002u;
}
void UnresolvedAddr::clear_address_field() {
  address_field_.ClearToEmptyNoArena(&::google::protobuf::internal::GetEmptyStringAlreadyInited());
  clear_has_address_field();
}
 const ::std::string& UnresolvedAddr::address_field() const {
  // @@protoc_insertion_point(field_get:cockroach.util.UnresolvedAddr.address_field)
  return address_field_.GetNoArena(&::google::protobuf::internal::GetEmptyStringAlreadyInited());
}
 void UnresolvedAddr::set_address_field(const ::std::string& value) {
  set_has_address_field();
  address_field_.SetNoArena(&::google::protobuf::internal::GetEmptyStringAlreadyInited(), value);
  // @@protoc_insertion_point(field_set:cockroach.util.UnresolvedAddr.address_field)
}
 void UnresolvedAddr::set_address_field(const char* value) {
  set_has_address_field();
  address_field_.SetNoArena(&::google::protobuf::internal::GetEmptyStringAlreadyInited(), ::std::string(value));
  // @@protoc_insertion_point(field_set_char:cockroach.util.UnresolvedAddr.address_field)
}
 void UnresolvedAddr::set_address_field(const char* value, size_t size) {
  set_has_address_field();
  address_field_.SetNoArena(&::google::protobuf::internal::GetEmptyStringAlreadyInited(),
      ::std::string(reinterpret_cast<const char*>(value), size));
  // @@protoc_insertion_point(field_set_pointer:cockroach.util.UnresolvedAddr.address_field)
}
 ::std::string* UnresolvedAddr::mutable_address_field() {
  set_has_address_field();
  // @@protoc_insertion_point(field_mutable:cockroach.util.UnresolvedAddr.address_field)
  return address_field_.MutableNoArena(&::google::protobuf::internal::GetEmptyStringAlreadyInited());
}
 ::std::string* UnresolvedAddr::release_address_field() {
  clear_has_address_field();
  return address_field_.ReleaseNoArena(&::google::protobuf::internal::GetEmptyStringAlreadyInited());
}
 void UnresolvedAddr::set_allocated_address_field(::std::string* address_field) {
  if (address_field != NULL) {
    set_has_address_field();
  } else {
    clear_has_address_field();
  }
  address_field_.SetAllocatedNoArena(&::google::protobuf::internal::GetEmptyStringAlreadyInited(), address_field);
  // @@protoc_insertion_point(field_set_allocated:cockroach.util.UnresolvedAddr.address_field)
}

#endif  // PROTOBUF_INLINE_NOT_IN_HEADERS

// @@protoc_insertion_point(namespace_scope)

}  // namespace util
}  // namespace cockroach

// @@protoc_insertion_point(global_scope)
