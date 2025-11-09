# MiniHadoop DFS

Sebuah Distributed File System (DFS) yang terinspirasi dari Hadoop, dikembangkan menggunakan Elixir sebagai bagian dari tugas mini project mata kuliah *Pemrograman Fungsional*.

## 🎯 Fitur Utama

- **Distributed Storage**: Berkas (file) dipecah menjadi blok dan didistribusikan ke beberapa DataNode.
- **Data Replication**: Setiap blok direplikasi ke sejumlah node untuk meningkatkan *fault tolerance*.
- **Fault Tolerance**: Sistem tetap dapat beroperasi meskipun terjadi kegagalan pada beberapa DataNode.
- **MapReduce Framework**: Mendukung pemrosesan data terdistribusi menggunakan model MapReduce.
- **Functional Programming**: Memanfaatkan prinsip pemrograman fungsional untuk meningkatkan reliabilitas dan *maintainability*.

## 🏗️ Arsitektur Sistem

### Model Master–Slave

- **NameNode (Master)**:
  - Mengelola metadata sistem berkas.
  - Melacak lokasi setiap blok yang tersimpan pada DataNode.
  - Mengoordinasikan operasi berkas dan tugas MapReduce.

- **DataNode + TaskTracker (Slave)**:
  - Menyimpan blok data pada penyimpanan lokal.
  - Menjalankan tugas MapReduce.
  - Mengirimkan *heartbeat* ke NameNode sebagai indikator status node.

### Komponen MapReduce

- **JobTracker**: Mengelola pengajuan pekerjaan dan penjadwalan tugas.
- **TaskTracker**: Mengeksekusi *map tasks* dan *reduce tasks*.
- **Pluggable Processing**: Pengguna dapat mendefinisikan fungsi map dan reduce sendiri.

## 🚀 Panduan Penggunaan

### 1. Persiapan Lingkungan

```bash
git clone <repository>
cd mini_hadoop
mix deps.get
```

### 2. Menjalankan Cluster Menggunakan Docker

```bash
docker-compose up --build
docker-compose up -d
```

### 3. Pengujian Cluster

Enter MasterNode interactive shell
```bash
docker exec -it mini_hadoop_master elixir --name client@master --cookie mini_hadoop_secret_cookie --remsh master@master.node
```

## 📚 Contoh Penggunaan API

### Operasi Berkas Dasar

```elixir
MiniHadoop.Client.store_file("document.txt", "Isi dokumen")
{:ok, content} = MiniHadoop.Client.read_file("document.txt")
files = MiniHadoop.Client.list_files()
MiniHadoop.Client.delete_file("document.txt")
```

### Pekerjaan MapReduce

```elixir
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
info = MiniHadoop.Client.cluster_info()
```

## 🎓 Aspek Pemrograman Fungsional

1. **Immutable Data Structures** — State cluster bersifat immutable untuk menjamin konsistensi.
2. **Pattern Matching** — Digunakan untuk komunikasi terdistribusi yang lebih aman dan jelas.
3. **Pure Functions** — Perilaku deterministik untuk penempatan blok dan partisi data.
4. **Higher-Order Functions** — Mendukung fleksibilitas fungsi map dan reduce yang dapat didefinisikan pengguna.
5. **Function Composition** — Alur data yang bersih dari penyimpanan → pemrosesan → keluaran.
6. **Side Effect Management** — Pemisahan operasi I/O dari logika utama.

## 📁 Struktur Proyek

```
mini_hadoop/
├── lib/
│   ├── mini_hadoop/
│   │   ├── client.ex
│   │   ├── application.ex
│   │   ├── master/
│   │   │   ├── name_node.ex
│   │   │   └── job_tracker.ex
│   │   ├── slave/
│   │   │   ├── data_node.ex
│   │   │   └── task_tracker.ex
│   │   └── common/
│   │       ├── block.ex
│   │       ├── job.ex
│   │       └── task.ex
├── docker-compose.yml
└── README.md
```

## 🔧 Konfigurasi

```elixir
@replication_factor 2
@heartbeat_interval 5_000
```

## 🛠️ Troubleshooting

```bash
docker-compose ps
docker-compose logs -f master
docker-compose down -v
docker-compose up --build
```

## 📄 Lisensi

MIT License — Tugas Mini Project Pemrograman Fungsional
