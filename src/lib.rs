use std::env::Args;

struct Master {}

struct Worker {}

struct MapReduce {
    // Size of input splits in MB
    block_size: u8,
    // Number of patitions (intermediate key)
    partitions: u16,
    map_workers: u16,
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

fn contains_flag(args: &Args, flag: &'static str) -> bool {
    let result: Vec<String> = args.filter(|arg| arg.trim().eq(flag)).collect();

    result.len() > 0
}

impl MapReduce {
    fn run(args: Args) {
        if contains_flag(&args, "--role=master") {}

        if contains_flag(&args, "--role=worker") {}
    }

    fn build() -> MapReduce {
        let mr = MapReduce {
            block_size: 64,
            map_workers: 0,
            partitions: 8,
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

    fn mapper(map: impl Fn(KeyValue) -> Vec<KeyValue>) {}

    fn reducer(reduce: impl Fn(String, Vec<String>) -> KeyValue) {}

    fn upload() {}
}
