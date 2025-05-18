# Feather

Feather project is a lightweight search engine & document-based nosql database written in Java, designed for efficient document indexing and retrieval.

## Current Features

- File-based segment storage system
- Memory-managed document processing
- Configurable merge policies
- Support for different field types (String, Numeric, Binary)
- Unicode support

## Getting Started

### Prerequisites

- Java 17 or higher
- Gradle 7.x

### Installation

Now Preparing.

## Architecture

### Storage System

Feather basically uses a segment-based storage system similar to Lucene. Each segment consists of multiple files:

- `.doc` - Document storage
- `.dic` - Term dictionary
- `.post` - Posting lists
- `.meta` - Segment metadata

### Memory Management

The system implements efficient memory management through:

- Document buffering
- Controlled segment creation
- Configurable flush thresholds

### File Format

Each segment file follows a common structure:

1. File Header (magic number, version, file type)
2. Content specific to each file type
3. Optional metadata

### File Structure

Each segment in Feather consists of four file types:

```
segment_1/
├── _1.doc  # Document Storage
│ └── [Document ID][Content Length][Content]...
├── _1.dic  # Term Dictionary
│ └── [Term][Document Frequency][Posting Position]...
├── _1.post # Posting Lists
│ └── [Document ID][Term Frequency][Position Info]...
└── _1.meta # Segment Metadata
    └── [Metadata][Deletion List]...
```

These Segment Files are immutable. Once created, they cannot be modified. It ensures thread-safe and concurrent operations.

**NOTE: Remember that the segment file features and structures are subject to change.**

For detailed binary format of each file type, see below:

#### Document File (.doc)

```
## File Header (25 bytes fixed)
├── Magic Number (4 bytes) # "FTHR" (0x46544852)
├── Version (4 bytes) # 1.0 (0x00010000)
├── File Type (1 byte) # DOC = 0x01
├── Record Count (4 bytes) # Number of documents
├── Timestamp (8 bytes) # Creation time
└── Header Size (4 bytes) # 25

## Document Records
├── Document 1
│ ├── Document ID (4 bytes)
│ ├── Content Length (4 bytes)
│ ├── Field Count (4 bytes)
│ └── Field List
│     ├── Field 1
│     │ ├── Field Name Length (2 bytes)
│     │ ├── Field Name (variable length, UTF-8)
│     │ ├── Field Type (1 byte)
│     │ └── Field Value
│     │     ├── String: [Length 4 bytes][Content UTF-8]
│     │     ├── Numeric: [8 bytes]
│     │     └── Binary: [Length 4 bytes][Content]
│     └── Field 2...
└── Document 2...
```

#### Dictionary File (.dic)

```
## File Header (25 bytes fixed)
├── Magic Number (4 bytes) # "FTHR" (0x46544852)
├── Version (4 bytes) # 1.0 (0x00010000)
├── File Type (1 byte) # DIC = 0x02
├── Record Count (4 bytes) # Number of terms
├── Timestamp (8 bytes) # Creation time
└── Header Size (4 bytes) # 25

## Dictionary Metadata
├── Term Records Position (8 bytes)
├── Term Index Position (8 bytes)
└── Block Count (4 bytes)

## Term Records Section
├── Term Record 1
│ ├── Field Length (2 bytes)
│ ├── Field Name (variable length, UTF-8)
│ ├── Text Length (2 bytes)
│ ├── Term Text (variable length, UTF-8)
│ ├── Document Frequency (4 bytes)
│ └── Posting Position (8 bytes)
└── Term Record 2...

## Term Index Section
├── Block Count (4 bytes)
├── Block Offsets Array
│ ├── Offset 1 (8 bytes)
│ ├── Offset 2 (8 bytes)
│ └── ... (8 bytes * Block Count)
└── Block Data
    ├── Block 1
    │ ├── Field Length (2 bytes)
    │ ├── Field Name (variable length, UTF-8)
    │ ├── Prefix Length (2 bytes)
    │ ├── Term Prefix (variable length, UTF-8)
    │ └── Record Position (8 bytes)
    └── Block 2...
```

- Uses block-based indexing (128 terms per block)
- Term prefixes are limited to 8 characters
- Binary search is performed on blocks
- Linear search within blocks
- All strings are UTF-8 encoded

#### Posting File (.post)

```
## File Header (25 bytes fixed)
├── Magic Number (4 bytes) # "FTHR" (0x46544852)
├── Version (4 bytes) # 1.0 (0x00010000)
├── File Type (1 byte) # POST = 0x03
├── Record Count (4 bytes) # Number of posting lists
├── Timestamp (8 bytes) # Creation time
└── Header Size (4 bytes) # 25

## Posting Lists Section
├── Posting List 1
│ ├── Document Count (4 bytes)
│ ├── Document Entry 1
│ │ ├── Delta Document ID (4 bytes) # Difference from previous doc ID
│ │ ├── Term Frequency (4 bytes)
│ │ ├── Position Count (4 bytes)
│ │ └── Positions
│ │     ├── Delta Position 1 (4 bytes) # Difference from previous position
│ │     ├── Delta Position 2 (4 bytes)
│ │     └── ... (4 bytes * Position Count)
│ ├── Document Entry 2...
│ └── ... (Document Count entries)
└── Posting List 2...
```

- Uses delta encoding for document IDs and positions
- Each posting list contains:
  - Document frequency (number of documents)
  - Per-document term frequency
  - Term positions within each document

#### Metadata File (.meta)

```
## File Header (25 bytes fixed)
├── Magic Number (4 bytes) # "FTHR" (0x46544852)
├── Version (4 bytes) # 1.0 (0x00010000)
├── File Type (1 byte) # META = 0x04
├── Record Count (4 bytes) # Document count
├── Timestamp (8 bytes) # Creation time
└── Header Size (4 bytes) # 25

## Segment Metadata Section
├── Creation Time (8 bytes) # Segment creation timestamp
├── Document Count (4 bytes) # Total number of documents
├── Min Document ID (4 bytes) # Smallest document ID in segment
├── Max Document ID (4 bytes) # Largest document ID in segment
└── Checksum (8 bytes) # Data integrity verification
```

## Configuration

Now Preparing.

## Project Status

- Current version: Not released.
- Upcoming features:
  - Efficient Indexing
  - Enhanced segment file merging and deletion
  - Querying including client-side Query
  - Aggregation
  - Analysis
  - ...
