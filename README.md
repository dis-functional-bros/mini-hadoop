# MiniHadoop

Sebuah Distributed File System (DFS) yang terinspirasi dari Hadoop, dikembangkan menggunakan Elixir sebagai bagian dari tugas mini project mata kuliah *Pemrograman Fungsional 25/26*.

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

## 🎓 Aspek Functional Programming 

### 1. **State Immutability** - Foundation for Predictable Distributed Systems

Immutability memastikan state program selalu predictable dan tidak berubah tanpa sepengetahuan developer. Setiap transformasi data bersifat eksplisit sehingga mudah dilacak dan dipahami alurnya. Hal ini menghilangkan bugs yang disebabkan oleh hidden state mutations dalam codebase yang besar.

```elixir
def handle_info(:proceed_to_completion, state) do
  result = state
    |> fetch_all_reduce_results()
    |> process_reduce_results()
    |> write_results_to_file(state.result_path, state.job.id)
    |> create_job_summary(state)
  final_state = notify_job_completion(state, result)
  {:stop, :normal, final_state}
end
```
Fungsi `write_results_to_file`, `create_job_summary`, dan `notify_job_completion` menggunakan state yang sama persis tanpa takut nilai berubah, karena immutability menjamin konsistensi data sepanjang execution pipeline.

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
defp execute_phase(state, phase_type, data_fetcher, task_generator, task_dispatcher, completion_checker) do
  with_counter_cleanup(state, phase_type, fn state ->
    state
    |> notify_phase_start(phase_type)
    |> data_fetcher.()                          # Injected data fetching logic
    |> (fn state_with_data ->
          # ... additional data processing logic
            Task.start(fn ->
              state_with_data
              |> Map.put(:current_chunk, chunk)
              |> then(&task_generator.(&1))     # Injected task generation
              |> then(&task_dispatcher.(&1))    # Injected task dispatch
            end)
          end)
          # ... additional steps
    |> completion_checker.()                    # Injected completion handling
  end)
end

# Use it like this
defp execute_map_phase(state) do
  execute_phase(
    state, :map, &fetch_participating_blocks/1, &generate_map_tasks/1,
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

Monadic execution pipeline memungkinkan kita mengeksekusi sequence operations dengan error handling yang elegant, di mana setiap step tidak perlu mengetahui status step sebelumnya. Pipeline akan otomatis short-circuit ketika terjadi error, dan error handling dilakukan secara terpusat di akhir tanpa mengganggu flow logic utama. Pendekatan ini sangat cocok untuk MiniHadoop yang memungkinkan user meng-inject custom functions dengan runtime behavior yang tidak predictable.

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

### 6. **No Shared Memory** — Elixir process model that ensures 

### 7. **Fault Tolerance & OTP** —  Combination of Immutability, Pure Function, Pattern Matching, Process Isolation, dan OTP Supervision Tree. Robust Fault Tolerance dan Error Handling


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

Di dalam Interactive Shell MasterNode kita dapat mengakses beberapa API yang telah disediakan

### Operasi Berkas Dasar

1. Menyimpan file ke dalam DFS
```elixir
# old
MiniHadoop.store_file("<nama_file_dalam_DFS>", "<path/ke/file>")
```
Disclaimer, pastikan master node memiliki akses ke path file yang ingin disimpan. 

2. Mengambil file dari DFS
```elixir
MiniHadoop.retrieve_file("<nama_file_dalam_DFS>")
```
File akan disimpan di direktori ./retrieve_result proyek

3. Menghapus file dari DFS
```elixir
MiniHadoop.delete_file("<nama_file_dalam_DFS>")
```
File akan dihapus dari DFS

4. Melihat progress operasi file
```elixir
MiniHadoop.file_op_info("id_operasi_file")
```

### MapReduce Job

1. Mendefinisikan Spesifikasi dan Menjalankan MapReduce Job
```elixir
{:ok, job_spec} = MiniHadoop.Models.JobSpec.create([
    job_name: "word_count_analysis",
    input_files: ["example.txt"],
    map_function: &MiniHadoop.Examples.WordCount.word_count_mapper/2,
    reduce_function: &MiniHadoop.Examples.WordCount.word_count_reducer/2
  ])
MiniHadoop.submit_job(job_spec)
```
Hasil MapReduce Job akan disimpan di direktori ./job_result proyek

nilai map_module dan reduce_module merupakan module yang mengimplementasikan behaviour MiniHadoop.Map.MapBehaviour dan MiniHadoop.Reduce.ReduceBehaviour

Anda dapat membuat implementasi costum untuk Map dan Reduce dengan membuat module baru yang mengimplementasikan behaviour MiniHadoop.Map.MapBehaviour dan MiniHadoop.Reduce.ReduceBehaviour.

```elixir
defmodule MiniHadoop.Map.Examples.CostumMap do
  @behaviour MiniHadoop.Map.MapBehaviour
  
  @impl true
  def map(data, context) do
    # Implementasi costum map
  end
end


defmodule MiniHadoop.Reduce.Examples.CostumReduce do
  @behaviour MiniHadoop.Reduce.ReduceBehaviour
  
  @impl true
  def reduce(data, context) do
    # Implementasi costum reduce
  end
end
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
