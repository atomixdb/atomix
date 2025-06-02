use nix::sched::{sched_setaffinity, CpuSet};
use nix::unistd::Pid;

pub fn restrict_to_cores(cores: &Vec<u32>) {
    let mut cpuset = CpuSet::new();
    for core in cores {
        cpuset.set(*core as usize).expect("failed to set core");
    }
    // Apply to current process
    sched_setaffinity(Pid::from_raw(0), &cpuset).expect("failed to set CPU affinity");
}
