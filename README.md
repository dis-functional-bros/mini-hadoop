# MiniHadoop

Sebuah Distributed File System (DFS) yang terinspirasi dari Hadoop, dikembangkan menggunakan Elixir sebagai bagian dari tugas mini project mata kuliah _Pemrograman Fungsional 25/26_.

## 🎯 Fitur Utama

- **Distributed Storage**: Berkas (file) dipecah menjadi blok dan didistribusikan ke beberapa DataNode.
- **Data Replication**: Setiap blok direplikasi ke sejumlah node untuk meningkatkan _fault tolerance_.
- **Fault Tolerance**: Sistem tetap dapat beroperasi meskipun terjadi kegagalan pada beberapa DataNode.
- **MapReduce Framework**: Mendukung pemrosesan data terdistribusi menggunakan model MapReduce.
- **Functional Programming**: Memanfaatkan prinsip pemrograman fungsional untuk meningkatkan reliabilitas dan _maintainability_.

## 🏗️ Arsitektur Sistem


MiniHadoop dibangun menggunakan arsitektur Master-Worker berbasis **Elixir OTP** yang memanfaatkan kekuatan *actor model* untuk konkurensi dan toleransi kesalahan.

*   **Master Node**:
    *   **MasterNode**: Mengelola pendaftaran worker (membership), penyeimbangan beban (*load balancing*) menggunakan struktur data `gb_trees`, dan pemantauan kesehatan cluster melalui mekanisme *heartbeat*.
    *   **FileOperation**: Menangani operasi sistem berkas terdistribusi (DFS), termasuk manajemen metadata blok dan strategi penempatan data.
    *   **ComputeOperation**: Mengelola siklus hidup job, antrian job (*job queue*), dan koordinasi status global job.
    *   **JobSupervisor**: `DynamicSupervisor` yang membuat dan mengawasi proses `JobRunner` yang terisolasi untuk setiap job yang aktif.

*   **Worker Node**:
    *   **WorkerNode**: Proses utama (*GenServer*) yang memelihara koneksi persisten ke Master dan melaporkan status kapasitas serta ketersediaan node.
    *   **TaskSupervisor**: `Task.Supervisor` yang mengeksekusi fungsi Map dan Reduce pengguna secara aman dan terisolasi (*sandboxed execution*).
    *   **RunnerAndStorageSupervisor**: `DynamicSupervisor` yang mengelola komponen pendukung untuk eksekusi tugas dan penyimpanan hasil sementara.

*   **Job Execution Flow**:
    *   **JobRunner**: Proses orkestrator (satu per job) yang bertanggung jawab memecah input menjadi beberapa task, menjadwalkan task ke worker yang tersedia, menangani kegagalan task (*fault tolerance*), dan melakukan agregasi hasil akhir.

## 🧠 Wawasan Pengembangan & Tantangan Teknis

### Kompleksitas Distribusi dan Manajemen State

Sistem terdistribusi pada dasarnya adalah **90% side effects**. Salah satu pembelajaran penting adalah memisahkan **coordination logic** (yang penuh side effects) dari **pure execution**. Dalam MiniHadoop, fetching data menjadi bagian koordinasi, sementara task execution dirancang sebagai pure function yang mudah di-_reason_ dan dikelola.

### Performa dan Optimasi Memory

Desain parallel process yang baik dapat memberikan dampak signifikan—dalam pengembangan ini, perbaikan kode berkualitas berhasil mengurangi penggunaan memori hingga **14x lipat**. Hal ini menunjukkan pentingnya:

- **Pemilihan struktur data yang tepat** daripada pendekatan "yang penting selesai"
- **Pemahaman mendalam terhadap BEAM VM** (scheduler, task, garbage collector)
- **Benchmark pada workload nyata**—microbenchmark bisa misleading dalam environment concurrent

### Tantangan Load Balancing

Implementasi awal menggunakan **round robin approach** untuk distribusi blok ternyata memiliki kelemahan. Pendekatan ini tidak mempertimbangkan load saat ini dari setiap node, sehingga tidak dapat bereaksi terhadap perubahan kondisi cluster. Ketika terjadi replikasi atau penghapusan blok yang tidak merata, distribusi menjadi tidak seimbang.

### Edge Cases dalam Sistem Terdistribusi

Sistem terdistribusi memiliki banyak edge cases yang harus ditangani, terutama dalam arsitektur master-slave:

- **Slave failure**: Node yang menyimpan sebagian data menjadi unreachable
- **Network issues**: Menyebabkan delay atau inkonsistensi operasi
- **Master failure**: Saat ini belum ditangani dalam implementasi
- **Data replication**: Diperlukan untuk mencegah data loss ketika node gagal

### Keunggulan Elixir/OTP untuk Distributed Systems

Elixir terbukti sangat powerful untuk menangani sistem terdistribusi karena:

- **Inter-node communication** melalui message passing yang aman dan sederhana
- **RPC calls** yang mudah untuk koordinasi antar server
- **Process isolation** yang murah namun powerful—spawning ratusan ribu lightweight process
- **Built-in fault tolerance** dengan supervision tree yang mengelola lifecycle process
- **Pattern matching** untuk handling failure recovery secara otomatis
- **Tail recursion optimization** oleh BEAM VM untuk efisiensi memori

### Prinsip Arsitektur Functional

- **State immutability**: State disimpan dalam GenServer/proses yang bertanggung jawab atas satu entitas logis
- **Message passing**: Menggantikan mutasi variabel global dengan komunikasi antar proses
- **Atomic operations**: Pattern matching pada RPC calls memberikan atomicity
- **Separation of concerns**: Memisahkan pure function dari side effects untuk maintainability

## 🎓 Aspek Functional Programming

### 1. **State Immutability** - Foundation for Predictable Distributed Systems

Immutability memastikan state program selalu predictable dan tidak berubah tanpa sepengetahuan developer. Setiap transformasi data bersifat eksplisit sehingga mudah dilacak dan dipahami alurnya. Hal ini menghilangkan bugs yang disebabkan oleh hidden state mutations dalam codebase yang besar.

```elixir
defp process_next_pending_task(state) do
  if concurrent_tasks_under_limit?(state) do
    case dequeue_task(state) do
      {task, new_state} when not is_nil(task) ->
        new_state
        |> execute_task_immediately(task)
        |> process_next_pending_task()
      {nil, new_state} ->
        new_state
    end
  else
    state
  end
end
```

Setiap fungsi menerima snapshot state yang immutable:

- dequeue_task(state) → Mengembalikan {task, new_state} di mana new_state adalah VERSI BARU tanpa task yang didequeue
- execute_task_immediately(new_state, task) → Bekerja dengan new_state tanpa mengubah state asli
- process_next_pending_task/1 → Rekursi menggunakan state TERBARU dari langkah sebelumnya

Dibandingkan dengan bahasa imperative, tanggung jawab sepenuhnya dibebankan kepada developer untuk memahami codebase dengan jelas semua operasi yang mungkin memutasi state:

```java
class Example {
  public String state;
  public void function() {
    this.state = "Correct";
    doComplexOperation();  // Operasi Kompleks

    if (state.equals("Correct")) {  // ❌ Assumption broken!
      System.out.println("State is Correct");
    }
  }
  public void doComplexOperation(){
    doAnotherComplexOperation();  // Memanggil operasi kompleks lainnya
  }

  ...

  public void doAnotherComplexOperation(){
    this.state = "State Unexpectedly Mutated";  // ❌ Hidden mutation tanpa sepengetahuan function original
  }
}
```

Hal ini sangat penting untuk kasus Distributed System seperti MiniHadoop yang kita buat, immutability menjadi fundamental requirement karena tanpa immutability, sistem terdistribusi akan rentan terhadap Non-Deterministic bugs dan prilaku inkonsisten yang sulit di-debug dan di-maintain.

### 2. **Pattern Matching** — Declarative Way to Handle Distributed System Communication.

Salah satu tantangan utama dalam sistem terdistribusi seperti MiniHadoop adalah bagaimana menangani komunikasi antar node (atau process di elixir) yang terlibat. Kompleksitas ini semakin meningkat seiring dengan bertambahnya tipe pesan dan kondisi bisnis, yang mana membuat kode sulit dimaintain dan rentan terhadap bugs. Pattern matching di Elixir memberikan solusi elegant dengan mengubah complex conditional logic menjadi declarative message routing yang predictable dan self-documenting.

```elixir
   # Task completes successfully
   def handle_info({task_ref, {:success, ref, task}}, state) when is_reference(task_ref) do
     # Do something
   end

   # Expected task failure
   def handle_info({task_ref, {:error, ref, task}}, state) when is_reference(task_ref) do
     # Do something
   end

   # Normal process shutdown for completed tasks (perform cleanup)
   def handle_info({:DOWN, ref, :process, _pid, :normal}, state) do
     # Do something
   end

   # Unexpected process shutdown, caused by unhandled failure
   def handle_info({:DOWN, ref, :process, _pid, reason}, state) when is_reference(ref) do
     # Do something
   end
```

Dibandingkan dengan pendekatan dengan bahasa imperative, developer harus secara manual melakukan type checking, casting, dan nested conditional logic yang menyulitkan maintenance dan rentan terhadap human error:

```java
public void handleMessage(Object message, SystemState state) {
    if (message instanceof TaskResult) {
        TaskResult result = (TaskResult) message;
        if (result.isSuccess()) {
            // More nested checking
        } else {
        }
    } else if (message instanceof ProcessSignal) {
        ProcessSignal signal = (ProcessSignal) message;
        if (signal.getType() == ProcessSignal.Type.SHUTDOWN) {
            if (signal.isNormal()) {
                cleanupResources(signal.getPid());
            } else {
                // More nested checking
            }
        }
    }
}
```

### 3. **Higher-Order Functions** — Functional Polymorphism for Distributed Workflow Orchestration

Higher-order functions memungkinkan functional polymorphism—runtime behavior variation melalui function composition, bukan class inheritance. Dalam distributed systems, pendekatan ini memungkinkan kita membangun workflow orchestration yang highly composable dengan meng-inject phase-specific behaviors sebagai parameters.

```elixir
defp execute_phase(state, phase_type, data_fetcher, input_formatter ,task_generator, task_dispatcher, completion_checker) do
  with_counter_cleanup(state, phase_type, fn state ->
    state
    |> notify_phase_start(phase_type)
    |> data_fetcher.()
    |> (fn state_with_data ->
        input_formatter.(state_with_data)
          |> Stream.each(fn chunk ->
            Task.start(fn ->
              state_with_data
              |> Map.put(:current_chunk, chunk)
              |> then(&task_generator.(&1))
              |> then(&task_dispatcher.(&1))
            end)
          end)
          |> Stream.run()
          state_with_data
        end).()
    |> completion_checker.()
  end)
end

# Use it like this
defp execute_map_phase(state) do
  execute_phase(
    state,
    :map,
    &fetch_participating_blocks/1,
    &block_list_formatter/1,
    &generate_map_tasks/1,
    &dispatch_tasks_with_state(&1, :map_tasks),
    &wait_for_phase_completion(&1, :map, :proceed_to_reduce)
  )
end
```

Dibandingkan dengan OOP Polymorphism yang mencapai behavior variation melalui class inheritance dan method overriding, functional polymorphism melalui higher-order functions memberikan flexibility yang sama tanpa complex hierarchy. Pendekatan OOP traditional membutuhkan rigid class hierarchy dengan fixed method signatures yang menyebabkan tight coupling antara behavior dan class structure:

```java
// OOP: Polymorphism via inheritance hierarchy
public abstract class PhaseExecutor {
    public abstract Data fetchData();           // Fixed method signatures
    public abstract List<Task> generateTasks(); // Rigid interface
}

public class MapPhaseExecutor extends PhaseExecutor {
    public Data fetchData() { /* map implementation */ }     // Tight coupling
    public List<Task> generateTasks() { /* map implementation */ }
}

// Harus buat subclass baru untuk setiap phase type
```

### 4. **Lazy Evaluation** — Efficient Large-Scale File Processing Through On-Demand Loading

Lazy evaluation memungkinkan MiniHadoop memproses file berukuran besar tanpa perlu meload seluruh konten ke memory sekaligus. Daripada membaca seluruh file dan membebani memory, sistem hanya meload dan memproses chunk file ketika benar-benar dibutuhkan oleh processing pipeline.

```elixir
result =
  File.stream!(task.file_path, block_size, [:read, :binary])
  |> Stream.with_index()
  |> Stream.chunk_every(@batch_size)
  |> Enum.reduce_while({:ok, [], 0}, fn chunk, {status, acc_blocks, processed_count} ->
    # Processing logic untuk setiap chunk
    chunk_result =
      chunk # Processing logic setiap block
      |> Task.async_stream(fn {data, index} ->
        block_id = "#{task.filename}_block_#{index}"
        # ... Sending block logic
        {:ok, {index, block_id}}
      end)
      |> Enum.reduce_while({:ok, [], 0}, accumulator_logic())

    case chunk_result do
      {:ok, chunk_blocks} -> # chunk success
      {:error, reason} -> # chunk failed
    end
  end)
```

Mekanisme Lazy Evaluation dalam Kode:

- `File.stream!` membuat lazy stream yang hanya membaca file ketika di-iterate
- `Stream.with_index` dan `Stream.chunk_every` mempertahankan lazy behavior
- `Enum.reduce_while` meng-consume stream secara bertahap, memproses satu chunk setiap kali
- Setiap block dalam chunk diproses secara independen dengan `Task.async_stream` untuk distribusi parallel

### 5. **Monadic Execution Pipeline** — Elegant Way to Execute Tasks Pipeline with Error Handling and Unknown Runtime Behavior

Monadic execution pipeline memungkinkan kita mengeksekusi sequence operations dengan error handling yang elegant, di mana setiap step tidak perlu mengetahui status step sebelumnya. Pipeline akan otomatis short-circuit ketika terjadi error, dan error handling dilakukan secara terpusat di akhir tanpa mengganggu flow logic utama. Pendekatan ini penting untuk program seperti MiniHadoop yang memungkinkan user meng-inject custom functions dengan runtime behavior yang unpredictable.

```elixir
# Monad operations
def pure(task), do: {:ok, task}
def error(reason), do: {:error, reason}

# Bind operation for chaining monadic operations
def bind({:ok, task}, func), do: func.(task)
def bind({:error, reason}, _func), do: {:error, reason}

@doc """
Task execution pipeline.
"""
@spec execute_task(ComputeTask.t()) :: execution_result()
def execute_task(task) do
  pure(task)
  |> bind(&fetch_task_data/1)
  |> bind(&execute_user_function/1)
  |> bind(&normalize_user_result/1)
  |> bind(&mark_task_completed/1)
end
```

dibandingkan dengan pendekatan imperative yang membutuhkan explicit error checking di setiap step:

```java
// Traditional imperative - error handling scattered everywhere
public ExecutionResult executeTask(Task task) {
    // Step 1: Must check error manually
    TaskData data = fetchTaskData(task);
    if (data == null) {
        return ExecutionResult.error("Data fetch failed");
    }

    // Step 2: Must check error again
    UserResult userResult = executeUserFunction(data);
    if (userResult.hasError()) {
        return ExecutionResult.error("User function failed: " + userResult.getError());
    }

    // Step 3: More manual error checking
    NormalizedResult normalized = normalizeUserResult(userResult);
    if (!normalized.isValid()) {
        return ExecutionResult.error("Normalization failed");
    }

    // Step 4: Final manual check
    if (!markTaskCompleted(normalized)) {
        return ExecutionResult.error("Completion marking failed");
    }

    return ExecutionResult.success(normalized);
}
```

Monadic pipeline menghilangkan boilerplate error handling yang repetitive, membuat code lebih clean dan focused pada business logic, sementara tetap maintaining comprehensive error propagation untuk custom user functions yang mungkin memiliki unpredictable runtime behavior.

### 6. **Pure-ish Functions** — Concurrent Execution Through Process Isolation Without Shared Memory

Pure-ish functions adalah fungsi-fungsi yang memiliki side effects yang terkontrol namun tetap mempertahankan sifat deterministic melalui isolasi memory yang ketat. Konsep ini berbeda dengan pure functions murni yang sama sekali tidak memiliki side effects, tetapi tetap mempertahankan prinsip penting: tidak ada shared mutable state antara concurrent executions.

Dalam paradigma functional programming seperti Elixir, setiap fungsi beroperasi pada data immutable dan setiap concurrent process memiliki memory space sendiri yang terisolasi. Ini menghilangkan kompleksitas synchronization yang biasanya menjadi tanggung jawab developer dalam paradigma imperative.

```elixir
  defp fetch_keys_concurrently(keys, map_files, storage_dir) do
    keys_set = MapSet.new(keys)

    map_files
    |> Task.async_stream(
      fn file ->
        # Isolasi sempurna: setiap task memiliki copy sendiri dari inputs
        process_single_map_file(file, keys_set, storage_dir)
      end,
      max_concurrency: @max_concurrent_file_reads,
      timeout: 30_000
    )
    |> Enum.reduce(%{}, fn
      {:ok, file_results}, acc ->
        merge_results_maps(acc, file_results)
      {:exit, _reason}, acc ->
        acc
    end)
  end
```

Fungsi process_single_map_filesebagai sebuah pure-ish function membaca map file yang terisolasi, setiap file merupakan unit pemrosesan independen yang tidak bergantung pada shared state. Meskipun melakukan I/O (side effect), fungsi ini tetap deterministic karena outputnya hanya bergantung pada konten file spesifik yang diproses, memungkinkan eksekusi concurrent tanpa synchronization.

Dalam sistem terdistribusi seperti MiniHadoop, Elixir Process Model menghilangkan kompleksitas shared memory dengan memberikan setiap task memory space terisolasi. Hal ini memungkinkan developer fokus pada business logic, sementara platform menjamin memory isolation dan safe concurrent execution.

### 7. **Fault Tolerance & OTP** — Robust Error Recovery Through Immutability, Process Isolation, and Supervision Trees

Fault tolerance dalam Elixir dibangun di atas fondasi konsep functional programming yang dikombinasikan dengan OTP (Open Telecom Platform) framework. Kombinasi _immutability_, _pure functions_, _pattern matching_, _process isolation_, dan _supervision trees_ menciptakan sistem yang resilient terhadap failures tanpa complex error handling boilerplate. Setiap komponen dapat fail dengan graceful recovery, sementara state tetap konsisten berkat sifat immutable data structures.

```elixir
# Spawn task with supervision
task_async = Task.Supervisor.async_nolink(MiniHadoop.ComputeTask.TaskSupervisor, fn ->
  case execute_task_logic(ComputeTask.mark_started(task)) do
    {:ok, task} -> {:success, ref, task}
    {:error,task} -> {:error, ref, task}
  end
end)

# Pattern matched the message
def handle_info({task_ref, {:success, ref, task}}, state) when is_reference(task_ref) do
  # Task success
end

def handle_info({:DOWN, ref, :process, _pid, :normal}, state) do
  # Task failed
end
```

Prinsip Fault Tolerance dalam Functional Programming:

- Immutability sebagai Safety Net: Data tidak bisa corrupt karena tidak ada in-place mutations
- Pure Functions untuk Deterministic Recovery: Fungsi dapat di-replay tanpa side effect yang unpredictable
- Pattern Matching untuk Explicit Error States: Error di-handle sebagai data, bukan exceptions yang tersembunyi
- Process Isolation untuk Fault Containment: Crash dalam satu process tidak mempengaruhi process lain

Dibandingkan dengan paradigma imperative seperti Java, fault tolerance sering bergantung pada complex exception handling dengan mutable state recovery:

```java
// Traditional imperative - manual state management during failures
public class TaskExecutor {
    private TaskState currentState;  // MUTABLE - prone to corruption
    private List<Task> pendingTasks; // MUTABLE - recovery complex

    public void executeTask(Task task) {
        try {
            currentState.markStarted(task);  // In-place modification
            TaskResult result = runTaskLogic(task);  // May throw
            currentState.markCompleted(task, result); // Another mutation

        } catch (Exception e) {
            // State mungkin sudah termutasi partial
            // Harus manual rollback ke consistent state
            currentState.rollback(task);  // Error-prone
            pendingTasks.add(task);       // Manual recovery logic

            // Re-throw atau log - error handling scattered
            if (e instanceof TimeoutException) {
                scheduleRetry(task);
            } else {
                markPermanentlyFailed(task);
            }
        }
    }
}
```

Dalam distributed computing framework seperti MiniHadoop, fault tolerance adalah kebutuhan fundamental, bukan sekadar fitur tambahan: _Map/Reduce tasks_ dapat mengalami kegagalan karena berbagai kondisi runtime seperti memory exhaustion, disk full, atau network timeout; _worker nodes_ mungkin crash atau menjadi unreachable selama job execution berlangsung; dan _data corruption_ atau malformed input dapat menyebabkan task failures yang tidak terduga.

## 🚀 Panduan Penggunaan

### 1. Persiapan Lingkungan

Pastikan anda telah menginstalasi docker

```bash
git clone <repository>
cd mini_hadoop
mix deps.get
```

### 2. Menjalankan Cluster Menggunakan Docker

```bash
docker compose up --build
docker compose up -d # Untuk menjalankan cluster di background
```

### 3. Mengakses Interactive Shell MasterNode

```bash
docker exec -it master_node iex --remsh master@master --cookie secret
```

## 📚 Contoh Penggunaan API

### Text File Generation and File Cleanup

1. Generate text file for word count job

```bash
python3 ./test_file/file_gen.py
```
Akan menghasilkan 3 file dengan ukuran variatif di direktori test_file . File tersebut adalah `small.txt` 4MB, `medium.txt` 16MB, dan `large.txt` 256MB.


2. Membersihkan file-file yang telah dihasilkan

```bash
sudo rm -rf ./data ./retrieved_files ./job_results ./temp ./logs
mkdir -p ./data ./retrieved_files ./job_results ./temp ./logs
```

Untuk memudahkan debugging, volume container telah di petakan ke dalam beberapa direktori khusus di dalam proyek. Untuk membersihkan file yang dihasilkan oleh program, kita dapat menggunakan perintah di atas (menghapus folder dan membuatnya kembali dalam keadaan kosong).


### Operasi Berkas Dasar

Di dalam Interactive Shell MasterNode kita dapat mengakses beberapa API yang telah disediakan

1. Menyimpan file ke dalam DFS

```elixir
MiniHadoop.store_file("<nama_file_dalam_DFS>", "<path/ke/file>")
# contoh : MiniHadoop.store_file("ps", "test.txt")
# file akan disimpan dengan nama "ps"
```

Disclaimer, pastikan master node memiliki akses ke path file yang ingin disimpan.

2. Mengambil file dari DFS

```elixir
MiniHadoop.retrieve_file("<nama_file_dalam_DFS>")
# contoh : MiniHadoop.retrieve_file("ps")
```

File akan disimpan di direktori ./retrieve_files proyek

3. Menghapus file dari DFS

```elixir
MiniHadoop.delete_file("<nama_file_dalam_DFS>")
# contoh : MiniHadoop.delete_file("ps")
```

File akan dihapus dari DFS

4. Melihat progress operasi file

```elixir
MiniHadoop.file_op_info("id_operasi_file")
# contoh : MiniHadoop.file_op_info("store_1")
```

### MapReduce Job

1. Mendefinisikan Spesifikasi dan Menjalankan MapReduce Job.

Ada beberapa keyword yang wajib untuk mendefinisikan spesifikasi job:

- `job_name`: Nama job (String).
- `input_files`: List nama file input yang akan diolah (List of String). File harus sudah tersimpan di DFS.
- `map_function`: Function yang akan dijalankan pada map phase.
  - Menerima input: `(line :: String.t(), context :: map())` atau `(line :: String.t())`.
  - Mengembalikan output: List of tuples `[{key, value}, ...]`.
- `reduce_function`: Function yang akan dijalankan pada reduce phase.
  - Menerima input: `({key, values}, context :: map())` atau `({key, values})`.
  - Mengembalikan output: Tuple `{key, result}`.

Optional keyword:

- `output_dir`: Direktori tujuan penyimpanan hasil job (String), bila tidak diberikan maka hasil akan disimpan di direktori default ./job_result proyek
- `map_context`: Map yang berisi parameter tambahan yang bisa diakses di dalam map function. 
- `reduce_context`: Map yang berisi parameter tambahan yang bisa diakses di dalam reduce function.
- `sort_result_opt`: Tuple yang menentukan opsi sorting hasil reduce phase, contoh: `{:value, :desc}` atau `{:key, :asc}`.  Element pertama tuple adalah field yang akan diurutkan (:key atau :value), element kedua adalah arah sorting (:asc atau :desc).

Untuk membuat spesifikasi job, gunakan fungsi `MiniHadoop.Models.JobSpec.create/1`. Fungsi ini akan memvalidasi semua parameter yang diberikan. Jika spesifikasi valid, fungsi akan mengembalikan tuple `{:ok, job_spec}`. Jika terdapat kesalahan parameter, fungsi akan mengembalikan `{:error, reason}`.

```elixir
{:ok, job_spec} = MiniHadoop.Models.JobSpec.create([
    job_name: "word_count_analysis",
    input_files: ["example.txt"],
    map_function: &MiniHadoop.Examples.WordCount.word_count_mapper/2,
    reduce_function: &MiniHadoop.Examples.WordCount.word_count_reducer/2
  ])
MiniHadoop.submit_job(job_spec)
```


### Output Job

Setiap job akan menghasilkan dua file output di direktori hasil:

1. **JSON File** (`.json`): Berisi hasil lengkap dalam format JSON object.
2. **Text File** (`.txt`): Berisi hasil dalam format baris per baris `key[tab]value`.

**JSON Output Example:**

```json
{
  "key_1": "value_1",
  "key_2": "value_2",
  "key_3": "value_3",
  "key_4": "value_4",
  "key_5": "value_5"
}
```

**Text Output Example:**

```text
# sorted/unsorted - Total: 5 entries
key_1	value_1
key_2	value_2
key_3	value_3
key_4	value_4
key_5	value_5
```


## 🔬 Contoh Algoritma MapReduce yang Didukung

MiniHadoop menyediakan implementasi untuk dua algoritma MapReduce klasik yang sering digunakan dalam distributed computing: **WordCount** dan **PageRank**. Kedua algoritma ini mendemonstrasikan kekuatan paradigm functional programming dalam menangani pemrosesan data terdistribusi.

### 1. WordCount Algorithm

WordCount adalah algoritma fundamental dalam ecosystem MapReduce yang menghitung frekuensi kemunculan setiap kata dalam kumpulan dokumen. Algoritma ini menjadi "Hello World" untuk distributed computing karena kesederhanaannya yang elegant namun representatif terhadap pola umum dalam big data processing.

#### Cara Kerja WordCount

**Map Phase:**

```elixir
def word_count_mapper(line, _context) do
  line
  |> String.downcase()
  |> String.replace(~r/[^\w\s]/, "")  # Remove punctuation
  |> String.split()
  |> Enum.map(fn word -> {word, 1} end)
end
```

**Reduce Phase:**

```elixir
def word_count_reducer({word, counts}, _context) do
  total_count = Enum.sum(counts)
  {word, total_count}
end
```

#### Functional Programming Advantages dalam WordCount

1. **Immutability**: Setiap transformasi string menghasilkan string baru, menghindari side effects
2. **Pure Functions**: Map dan reduce functions deterministic—input yang sama selalu menghasilkan output yang sama
3. **Lazy Evaluation**: File besar diproses secara streaming tanpa memuat seluruh konten ke memory
4. **Composability**: Pipeline transformasi dapat di-compose dengan operator `|>`

#### Contoh Penggunaan WordCount

```elixir
# Store input file
MiniHadoop.store_file("document.txt", "/path/to/large/document.txt")

# Create and submit WordCount job
{:ok, job_spec} = MiniHadoop.Models.JobSpec.create([
  job_name: "word_frequency_analysis",
  input_files: ["document.txt"],
  map_function: &MiniHadoop.Examples.WordCount.word_count_mapper/2,
  reduce_function: &MiniHadoop.Examples.WordCount.word_count_reducer/2
])

MiniHadoop.submit_job(job_spec)
```

### 2. PageRank Algorithm

PageRank adalah algoritma yang dikembangkan oleh Larry Page dan Sergey Brin (founders Google) untuk menentukan importance/authority sebuah web page berdasarkan link structure. Algoritma ini revolusioner dalam search engine technology dan menjadi fondasi awal Google Search.

#### Mathematical Foundation

PageRank menggunakan konsep **random walk** pada graph structure. Formula matematis:

$$PR(A) = \frac{1-d}{N} + d \sum_{i=1}^{M} \frac{PR(T_i)}{C(T_i)}$$

Dimana:

- $PR(A)$ = PageRank value untuk page A
- $d$ = damping factor (biasanya 0.85)
- $N$ = total number of pages
- $T_i$ = pages yang memiliki link ke page A
- $C(T_i)$ = number of outbound links dari page $T_i$
- $M$ = number of pages yang link ke A

#### Implementation dalam MiniHadoop

**Map Phase (First Iteration):** (Code simplified)

```elixir
def pagerank_mapper(line, context) do
  damping_factor = Map.get(context, :damping_factor, 0.85)
  total_pages = Map.get(context, :total_pages, 1)

  [from_page | to_pages] = String.split(line, "\t")

  # Initial PageRank distribution
  initial_rank = (1.0 - damping_factor) / total_pages

  # Distribute rank to outbound links
  rank_per_link = initial_rank / length(to_pages)

  result = Enum.map(to_pages, fn to_page ->
    {to_page, rank_per_link}
  end)

  # Add baseline contribution for source page
  [{from_page, (1 - damping_factor) / total_pages} | result]
end
```

**Map Phase (Subsequent Iterations):** (Code simplified)

```elixir
def pagerank_mapper(line, context) do
  damping_factor = Map.get(context, :damping_factor, 0.85)
  pagerank_file = Map.get(context, :pagerank_file)

  # Load previous iteration results
  previous_ranks = load_previous_pagerank(pagerank_file)

  [from_page | to_pages] = String.split(line, "\t")
  current_rank = Map.get(previous_ranks, from_page, 0.0)

  # Distribute current rank to outbound links
  rank_per_link = (damping_factor * current_rank) / length(to_pages)

  result = Enum.map(to_pages, fn to_page ->
    {to_page, rank_per_link}
  end)

  # Add baseline contribution for source page
  [{from_page, (1 - damping_factor) / total_pages} | result]
end
```

**Reduce Phase:**

```elixir
def pagerank_reducer({page, incoming_ranks}, _context) do
  # Sum all incoming rank contributions, already include damping factor contribution
  final_rank = Enum.sum(incoming_ranks)

  {page, final_rank}
end
```

#### Iterative Computation Process

PageRank membutuhkan iterative computation hingga convergence. MiniHadoop mendukung multi-iteration jobs:

```elixir
# First iteration
{:ok, first_job} = MiniHadoop.Models.JobSpec.create([
  job_name: "pagerank_iteration_1",
  input_files: ["web_links.tsv"],
  map_function: &MiniHadoop.Examples.PageRank.pagerank_mapper/2,
  reduce_function: &MiniHadoop.Examples.PageRank.pagerank_reducer/2,
  map_context: %{
    damping_factor: 0.85,
    total_pages: 41332
  },
  sort_result_opt: {:value, :desc}
])

MiniHadoop.submit_job(first_job)

# Second iteration (menggunakan hasil dari iterasi pertama)
{:ok, second_job} = MiniHadoop.Models.JobSpec.create([
  job_name: "pagerank_iteration_2",
  input_files: ["web_links.tsv"],
  map_function: &MiniHadoop.Examples.PageRank.pagerank_mapper/2,
  reduce_function: &MiniHadoop.Examples.PageRank.pagerank_reducer/2,
  map_context: %{
    pagerank_file: "/shared/page_rank_iter_1.json",
    damping_factor: 0.85,
    total_pages: 41332
  },
  sort_result_opt: {:value, :desc}
])

MiniHadoop.submit_job(second_job)
```

#### Input Format

PageRank expects tab-separated input format:

```
page1	page2	page3	page4
page2	page1	page5
page3	page1	page2	page6
```

#### Functional Programming Benefits dalam PageRank

1. **Stateless Iterations**: Setiap iterasi adalah pure function dari hasil iterasi sebelumnya
2. **Immutable Graph Structure**: Link graph tidak berubah selama computation
3. **Composable Transformations**: Rank calculations dapat di-compose dengan mathematical operations
4. **Distributed Convergence**: Convergence checking dapat dilakukan secara distributed tanpa global state
5. **Fault Tolerance**: Jika iteration gagal, dapat di-restart dari checkpoint terakhir tanpa corruption

#### Convergence dan Optimization

Typically, PageRank converge setelah 10-50 iterasi tergantung pada:

- **Graph size dan density**
- **Desired precision tolerance**
- **Damping factor value**

```elixir
# Helper function untuk check convergence
def check_convergence(previous_ranks, current_ranks, tolerance \\ 0.0001) do
  differences =
    Map.merge(previous_ranks, current_ranks, fn _key, old_val, new_val ->
      abs(new_val - old_val)
    end)

  max_difference = differences |> Map.values() |> Enum.max()
  max_difference < tolerance
end
```

**Output Example:**

Unsorted json object result
```json
{
  "homepage.html": 0.384729,
  "blog_post_1.html": 0.067123,
  "products.html": 0.054967,
  "main_article.html": 0.297156,
  "index.html": 0.201847,
  "about.html": 0.156892,
  "contact.html": 0.089234,
  "services.html": 0.043821,
  "news.html": 0.038194,
  "faq.html": 0.032156
}
```

Sorted/unsorted text file result
```text
# sorted/unsorted - Total: 10 entries
homepage.html	0.384729
main_article.html	0.297156
index.html	0.201847
about.html	0.156892
contact.html	0.089234
blog_post_1.html	0.067123
products.html	0.054967
services.html	0.043821
news.html	0.038194
faq.html	0.032156
```

---

### 3. Perbandingan Algorithmic Complexity

| Algoritma | Time Complexity | Space Complexity          | Convergence         | Use Cases                            |
| --------- | --------------- | ------------------------- | ------------------- | ------------------------------------ |
| WordCount | O(n)            | O(k) where k=unique words | Single pass         | Text analysis, log processing        |
| PageRank  | O(i × n × m)    | O(n)                      | Multiple iterations | Web ranking, social network analysis |

**Notes:**

- n = input size, m = average links per page, i = iterations until convergence
- k = vocabulary size (typically k << n for natural language)

### 4. Advanced Usage Patterns

#### Custom MapReduce Functions

Anda dapat mengimplementasikan algoritma MapReduce custom dengan mengikuti pattern yang sama:

```elixir
defmodule MyCustomAlgorithm do
  def custom_mapper(input, context) do
    # Your custom map logic
    # Must return list of {key, value} tuples
  end

  def custom_reducer({key, values}, context) do
    # Your custom reduce logic
    # Must return {key, reduced_value}
  end
end

# Usage
{:ok, job_spec} = MiniHadoop.Models.JobSpec.create([
  job_name: "custom_analysis",
  input_files: ["data.txt"],
  map_function: &MyCustomAlgorithm.custom_mapper/2,
  reduce_function: &MyCustomAlgorithm.custom_reducer/2,
  map_context: %{custom_param: "value"},
  reduce_context: %{another_param: 42}
])
```

#### Context-Aware Processing

Context parameters memungkinkan dynamic behavior dalam map/reduce functions:

```elixir
def context_aware_mapper(line, context) do
  case Map.get(context, :processing_mode) do
    "word_count" -> word_count_logic(line)
    "ngram_analysis" -> ngram_logic(line, context[:n])
    "sentiment" -> sentiment_analysis(line, context[:model])
  end
end
```

Implementasi algoritma-algoritma ini dalam MiniHadoop mendemonstrasikan kekuatan functional programming untuk distributed computing: **immutability** memastikan consistency across distributed nodes, **pure functions** memungkinkan reliable parallel execution, dan **composability** memberikan flexibility dalam membangun complex data processing pipelines.

## 📄 Lisensi

MIT License — Tugas Mini Project Pemrograman Fungsional
