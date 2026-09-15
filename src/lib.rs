use std::env::Args;

use kube::Client;

struct Master {}

struct Worker {}

struct MapReduce {
    // Size of input splits in MB
    block_size: u8,
    // Number of patitions (intermediate key)
    partitions: u16,
    map_workers: u16,
    client: Client,
}

struct KeyValue {
    key: String,
    value: String,
}

trait Mapper {
    fn map(kv: KeyValue) -> Vec<KeyValue>;
}

trait Reducer {
    fn reduce(key: String, values: Vec<String>) -> KeyValue;
}

fn contains_flag(args: &Vec<String>, flag: &'static str) -> bool {
    let clone = args.clone();
    let result: Vec<String> = clone
        .into_iter()
        .filter(|arg| arg.trim().eq(flag))
        .collect();

    result.len() > 0
}

impl MapReduce {
    fn run(
        &self,
        args: Args,
        mapper: impl Fn(KeyValue) -> Vec<KeyValue>,
        reducer: impl Fn(String, Vec<String>) -> KeyValue,
    ) {
        let collection: Vec<String> = args.collect();
        if contains_flag(&collection, "--role=master") {
            self.start_master();
            self.calculate_splits();
            self.assign_tasks();
            self.wait_job();
            return;
        }

        if contains_flag(&collection, "--role=worker") {
            self.start_worker(mapper, reducer);
            self.wait_task();
            return;
        }

        let bin_name = collection.get(0).unwrap();
        self.upload_binary(bin_name);
        self.request_node(format!("{} --role=master", bin_name));
        self.request_nodes(
            self.map_workers + self.partitions,
            format!("{} --role=worker", bin_name),
        );
        self.wait_job();
    }

    async fn build() -> MapReduce {
        let mr = MapReduce {
            block_size: 64,
            map_workers: 0,
            partitions: 8,
            client: Client::try_default().await.unwrap(),
        };

        mr
    }

    fn split(&mut self, size: u8) -> &mut MapReduce {
        self.block_size = size;
        self
    }

    fn map_workers(&mut self, amount: u16) -> &mut MapReduce {
        self.map_workers = amount;
        self
    }

    fn partitions(&mut self, number: u16) -> &mut MapReduce {
        self.partitions = number;
        self
    }

    fn upload_binary(&self, name: &String) {}

    fn start_master(&self) {}

    fn start_worker(
        &self,
        mapper: impl Fn(KeyValue) -> Vec<KeyValue>,
        reducer: impl Fn(String, Vec<String>) -> KeyValue,
    ) {
    }

    fn request_node(&self, cmd: String) {}

    fn request_nodes(&self, amount: u16, cmd: String) {}

    fn wait_job(&self) {}
    fn wait_task(&self) {}

    fn calculate_splits(&self) {}

    fn assign_tasks(&self) {}
}
