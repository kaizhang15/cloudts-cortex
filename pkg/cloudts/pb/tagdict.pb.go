// protos/tagdict.proto

// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.36.6
// 	protoc        v4.25.3
// source: tagdict.proto

package pb

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
	unsafe "unsafe"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type NodeType int32

const (
	NodeType_ROOT      NodeType = 0
	NodeType_METRIC    NodeType = 1
	NodeType_TAG_NAME  NodeType = 2
	NodeType_TAG_VALUE NodeType = 3
)

// Enum value maps for NodeType.
var (
	NodeType_name = map[int32]string{
		0: "ROOT",
		1: "METRIC",
		2: "TAG_NAME",
		3: "TAG_VALUE",
	}
	NodeType_value = map[string]int32{
		"ROOT":      0,
		"METRIC":    1,
		"TAG_NAME":  2,
		"TAG_VALUE": 3,
	}
)

func (x NodeType) Enum() *NodeType {
	p := new(NodeType)
	*p = x
	return p
}

func (x NodeType) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (NodeType) Descriptor() protoreflect.EnumDescriptor {
	return file_tagdict_proto_enumTypes[0].Descriptor()
}

func (NodeType) Type() protoreflect.EnumType {
	return &file_tagdict_proto_enumTypes[0]
}

func (x NodeType) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use NodeType.Descriptor instead.
func (NodeType) EnumDescriptor() ([]byte, []int) {
	return file_tagdict_proto_rawDescGZIP(), []int{0}
}

type TagDictSnapshot struct {
	state         protoimpl.MessageState       `protogen:"open.v1"`
	Nodes         []*TagDictSnapshot_TrieNode  `protobuf:"bytes,1,rep,name=nodes,proto3" json:"nodes,omitempty"`
	Partitions    []*TagDictSnapshot_Partition `protobuf:"bytes,2,rep,name=partitions,proto3" json:"partitions,omitempty"`
	NextEncoding  uint32                       `protobuf:"varint,3,opt,name=next_encoding,json=nextEncoding,proto3" json:"next_encoding,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *TagDictSnapshot) Reset() {
	*x = TagDictSnapshot{}
	mi := &file_tagdict_proto_msgTypes[0]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *TagDictSnapshot) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*TagDictSnapshot) ProtoMessage() {}

func (x *TagDictSnapshot) ProtoReflect() protoreflect.Message {
	mi := &file_tagdict_proto_msgTypes[0]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use TagDictSnapshot.ProtoReflect.Descriptor instead.
func (*TagDictSnapshot) Descriptor() ([]byte, []int) {
	return file_tagdict_proto_rawDescGZIP(), []int{0}
}

func (x *TagDictSnapshot) GetNodes() []*TagDictSnapshot_TrieNode {
	if x != nil {
		return x.Nodes
	}
	return nil
}

func (x *TagDictSnapshot) GetPartitions() []*TagDictSnapshot_Partition {
	if x != nil {
		return x.Partitions
	}
	return nil
}

func (x *TagDictSnapshot) GetNextEncoding() uint32 {
	if x != nil {
		return x.NextEncoding
	}
	return 0
}

type TagDictSnapshot_TrieNode struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	PathFragment  string                 `protobuf:"bytes,1,opt,name=path_fragment,json=pathFragment,proto3" json:"path_fragment,omitempty"` // 当前路径段（如 "cpu" 或 "core1"）
	Encoding      uint32                 `protobuf:"varint,2,opt,name=encoding,proto3" json:"encoding,omitempty"`
	FullPath      string                 `protobuf:"bytes,3,opt,name=full_path,json=fullPath,proto3" json:"full_path,omitempty"`   // 完整路径（如 "cpu=core1"）
	Type          NodeType               `protobuf:"varint,4,opt,name=type,proto3,enum=cloudts.pb.NodeType" json:"type,omitempty"` // 节点类型枚举
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *TagDictSnapshot_TrieNode) Reset() {
	*x = TagDictSnapshot_TrieNode{}
	mi := &file_tagdict_proto_msgTypes[1]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *TagDictSnapshot_TrieNode) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*TagDictSnapshot_TrieNode) ProtoMessage() {}

func (x *TagDictSnapshot_TrieNode) ProtoReflect() protoreflect.Message {
	mi := &file_tagdict_proto_msgTypes[1]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use TagDictSnapshot_TrieNode.ProtoReflect.Descriptor instead.
func (*TagDictSnapshot_TrieNode) Descriptor() ([]byte, []int) {
	return file_tagdict_proto_rawDescGZIP(), []int{0, 0}
}

func (x *TagDictSnapshot_TrieNode) GetPathFragment() string {
	if x != nil {
		return x.PathFragment
	}
	return ""
}

func (x *TagDictSnapshot_TrieNode) GetEncoding() uint32 {
	if x != nil {
		return x.Encoding
	}
	return 0
}

func (x *TagDictSnapshot_TrieNode) GetFullPath() string {
	if x != nil {
		return x.FullPath
	}
	return ""
}

func (x *TagDictSnapshot_TrieNode) GetType() NodeType {
	if x != nil {
		return x.Type
	}
	return NodeType_ROOT
}

type TagDictSnapshot_Partition struct {
	state          protoimpl.MessageState `protogen:"open.v1"`
	PartitionId    uint64                 `protobuf:"varint,1,opt,name=partition_id,json=partitionId,proto3" json:"partition_id,omitempty"`
	TagFrequencies map[uint32]uint32      `protobuf:"bytes,2,rep,name=tag_frequencies,json=tagFrequencies,proto3" json:"tag_frequencies,omitempty" protobuf_key:"varint,1,opt,name=key" protobuf_val:"varint,2,opt,name=value"` // encoding -> count
	unknownFields  protoimpl.UnknownFields
	sizeCache      protoimpl.SizeCache
}

func (x *TagDictSnapshot_Partition) Reset() {
	*x = TagDictSnapshot_Partition{}
	mi := &file_tagdict_proto_msgTypes[2]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *TagDictSnapshot_Partition) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*TagDictSnapshot_Partition) ProtoMessage() {}

func (x *TagDictSnapshot_Partition) ProtoReflect() protoreflect.Message {
	mi := &file_tagdict_proto_msgTypes[2]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use TagDictSnapshot_Partition.ProtoReflect.Descriptor instead.
func (*TagDictSnapshot_Partition) Descriptor() ([]byte, []int) {
	return file_tagdict_proto_rawDescGZIP(), []int{0, 1}
}

func (x *TagDictSnapshot_Partition) GetPartitionId() uint64 {
	if x != nil {
		return x.PartitionId
	}
	return 0
}

func (x *TagDictSnapshot_Partition) GetTagFrequencies() map[uint32]uint32 {
	if x != nil {
		return x.TagFrequencies
	}
	return nil
}

var File_tagdict_proto protoreflect.FileDescriptor

const file_tagdict_proto_rawDesc = "" +
	"\n" +
	"\rtagdict.proto\x12\n" +
	"cloudts.pb\"\xa6\x04\n" +
	"\x0fTagDictSnapshot\x12:\n" +
	"\x05nodes\x18\x01 \x03(\v2$.cloudts.pb.TagDictSnapshot.TrieNodeR\x05nodes\x12E\n" +
	"\n" +
	"partitions\x18\x02 \x03(\v2%.cloudts.pb.TagDictSnapshot.PartitionR\n" +
	"partitions\x12#\n" +
	"\rnext_encoding\x18\x03 \x01(\rR\fnextEncoding\x1a\x92\x01\n" +
	"\bTrieNode\x12#\n" +
	"\rpath_fragment\x18\x01 \x01(\tR\fpathFragment\x12\x1a\n" +
	"\bencoding\x18\x02 \x01(\rR\bencoding\x12\x1b\n" +
	"\tfull_path\x18\x03 \x01(\tR\bfullPath\x12(\n" +
	"\x04type\x18\x04 \x01(\x0e2\x14.cloudts.pb.NodeTypeR\x04type\x1a\xd5\x01\n" +
	"\tPartition\x12!\n" +
	"\fpartition_id\x18\x01 \x01(\x04R\vpartitionId\x12b\n" +
	"\x0ftag_frequencies\x18\x02 \x03(\v29.cloudts.pb.TagDictSnapshot.Partition.TagFrequenciesEntryR\x0etagFrequencies\x1aA\n" +
	"\x13TagFrequenciesEntry\x12\x10\n" +
	"\x03key\x18\x01 \x01(\rR\x03key\x12\x14\n" +
	"\x05value\x18\x02 \x01(\rR\x05value:\x028\x01*=\n" +
	"\bNodeType\x12\b\n" +
	"\x04ROOT\x10\x00\x12\n" +
	"\n" +
	"\x06METRIC\x10\x01\x12\f\n" +
	"\bTAG_NAME\x10\x02\x12\r\n" +
	"\tTAG_VALUE\x10\x03B5Z3github.com/kaizhang15/cloudts-cortex/pkg/cloudts/pbb\x06proto3"

var (
	file_tagdict_proto_rawDescOnce sync.Once
	file_tagdict_proto_rawDescData []byte
)

func file_tagdict_proto_rawDescGZIP() []byte {
	file_tagdict_proto_rawDescOnce.Do(func() {
		file_tagdict_proto_rawDescData = protoimpl.X.CompressGZIP(unsafe.Slice(unsafe.StringData(file_tagdict_proto_rawDesc), len(file_tagdict_proto_rawDesc)))
	})
	return file_tagdict_proto_rawDescData
}

var file_tagdict_proto_enumTypes = make([]protoimpl.EnumInfo, 1)
var file_tagdict_proto_msgTypes = make([]protoimpl.MessageInfo, 4)
var file_tagdict_proto_goTypes = []any{
	(NodeType)(0),                     // 0: cloudts.pb.NodeType
	(*TagDictSnapshot)(nil),           // 1: cloudts.pb.TagDictSnapshot
	(*TagDictSnapshot_TrieNode)(nil),  // 2: cloudts.pb.TagDictSnapshot.TrieNode
	(*TagDictSnapshot_Partition)(nil), // 3: cloudts.pb.TagDictSnapshot.Partition
	nil,                               // 4: cloudts.pb.TagDictSnapshot.Partition.TagFrequenciesEntry
}
var file_tagdict_proto_depIdxs = []int32{
	2, // 0: cloudts.pb.TagDictSnapshot.nodes:type_name -> cloudts.pb.TagDictSnapshot.TrieNode
	3, // 1: cloudts.pb.TagDictSnapshot.partitions:type_name -> cloudts.pb.TagDictSnapshot.Partition
	0, // 2: cloudts.pb.TagDictSnapshot.TrieNode.type:type_name -> cloudts.pb.NodeType
	4, // 3: cloudts.pb.TagDictSnapshot.Partition.tag_frequencies:type_name -> cloudts.pb.TagDictSnapshot.Partition.TagFrequenciesEntry
	4, // [4:4] is the sub-list for method output_type
	4, // [4:4] is the sub-list for method input_type
	4, // [4:4] is the sub-list for extension type_name
	4, // [4:4] is the sub-list for extension extendee
	0, // [0:4] is the sub-list for field type_name
}

func init() { file_tagdict_proto_init() }
func file_tagdict_proto_init() {
	if File_tagdict_proto != nil {
		return
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: unsafe.Slice(unsafe.StringData(file_tagdict_proto_rawDesc), len(file_tagdict_proto_rawDesc)),
			NumEnums:      1,
			NumMessages:   4,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_tagdict_proto_goTypes,
		DependencyIndexes: file_tagdict_proto_depIdxs,
		EnumInfos:         file_tagdict_proto_enumTypes,
		MessageInfos:      file_tagdict_proto_msgTypes,
	}.Build()
	File_tagdict_proto = out.File
	file_tagdict_proto_goTypes = nil
	file_tagdict_proto_depIdxs = nil
}
