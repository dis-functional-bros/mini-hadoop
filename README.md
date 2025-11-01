# MiniHadoop DFS

Sebuah Distributed File System (DFS) yang terinspirasi dari Hadoop, dibangun dengan Elixir sebagai tugas mini project mata kuliah Pemrograman Fungsional.

## 🎯 Fitur Utama

- **Distributed Storage**: File dipecah menjadi blocks dan didistribusikan ke multiple DataNodes
- **Data Replication**: Setiap block direplikasi ke beberapa node untuk fault tolerance  
- **Fault Tolerance**: Sistem tetap beroperasi bahkan jika beberapa DataNode gagal
- **MapReduce Framework**: Distributed data processing dengan model MapReduce
- **Functional Programming**: Menggunakan prinsip FP untuk reliability dan maintainability

## 🏗️ Arsitektur Sistem

### Master-Slave Architecture
- **NameNode (Master)**: 
  - Mengelola metadata file system
  - Melacak lokasi blocks di seluruh DataNodes
  - Mengoordinasi operasi file dan MapReduce jobs
  
- **DataNode + TaskTracker (Slaves)**: 
  - Menyimpan data blocks di local storage
  - Menjalankan MapReduce tasks
  - Mengirim heartbeat ke NameNode

### Komponen MapReduce
- **JobTracker**: Mengelola job submission dan task scheduling
- **TaskTracker**: Mengeksekusi map dan reduce tasks
- **Pluggable Processing**: User-defined map dan reduce functions

## 🚀 Quick Start

### 1. Setup Environment

```bash
# Clone dan setup project
git clone <repository>
cd mini_hadoop

# Install dependencies
mix deps.get
```

### 2. Menjalankan Cluster dengan Docker

```bash
# Build dan start cluster (1 master + 3 slaves)
docker-compose up --build

# Atau jalankan di background
docker-compose up -d
```

### 3. Testing Cluster

```bash
# Test operasi dasar
docker exec -it mini_hadoop_master iex --name client@master --cookie mini_hadoop_secret_cookie
```

## 📚 API Examples

### Operasi File Dasar

```elixir
# Store file 
MiniHadoop.Client.store_file("document.txt", "Content file")

# Read file
{:ok, content} = MiniHadoop.Client.read_file("document.txt")

# List semua file
files = MiniHadoop.Client.list_files()

# Delete file
MiniHadoop.Client.delete_file("document.txt")
```

### MapReduce Jobs

```elixir
# Submit MapReduce job
job_spec = %{
  input_path: "/data/input",
  output_path: "/data/output", 
  map_function: &word_count_map/2,
  reduce_function: &word_count_reduce/3
}

MiniHadoop.JobTracker.submit_job(job_spec)
```

### Monitoring Cluster

```elixir
# Dapatkan informasi cluster
info = MiniHadoop.Client.cluster_info()
```

## 🎓 Aspek Pemrograman Fungsional

### 1. Immutable Data Structures
State cluster bersifat immutable, memastikan konsistensi data across distributed nodes

### 2. Pattern Matching  
Robust message handling untuk distributed communication dengan pattern matching

### 3. Pure Functions
Deterministic behavior untuk block placement dan data partitioning

### 4. Higher-Order Functions
Extensible MapReduce framework dengan pluggable processing functions

### 5. Function Composition
Clean data flow pipelines dari storage → processing → output

### 6. Side Effect Management
Isolasi I/O operations dan network calls dari business logic

## 📁 Project Structure

```
mini_hadoop/
├── lib/
│   ├── mini_hadoop/
│   │   ├── client.ex                 # Main DFS API
│   │   ├── application.ex            # Application supervisor
│   │   ├── master/
│   │   │   ├── name_node.ex          # Metadata management
│   │   │   └── job_tracker.ex        # MapReduce job coordination
│   │   ├── slave/
│   │   │   ├── data_node.ex          # Block storage
│   │   │   └── task_tracker.ex       # Task execution
│   │   └── common/
│   │       ├── block.ex              # Block utilities
│   │       ├── job.ex                # Job specifications
│   │       └── task.ex               # Task definitions
├── docker-compose.yml                # Cluster configuration
└── README.md
```

## 🔧 Konfigurasi

### Settings Default
```elixir
@replication_factor 2     # Replikasi setiap block
@heartbeat_interval 5_000 # 5 detik
```

## 🛠️ Troubleshooting

```bash
# Cek status containers
docker-compose ps

# Monitor logs
docker-compose logs -f master

# Clean restart
docker-compose down -v
docker-compose up --build
```

## 📄 License

MIT License - Tugas Mini Project Pemrograman Fungsional