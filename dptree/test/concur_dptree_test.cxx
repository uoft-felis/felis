#include <iostream>
#include <sys/time.h>
#include <sys/resource.h>
#include <stdio.h>
#include <unistd.h>
#include <fstream>
#include <concur_dptree.hpp>
#include <stx/btree_map>
#include <unordered_set>
#include <random>
#include <vector>
#include <util.h>
#include <btreeolc.hpp>
#include <key_entry_types.h>

using namespace std;

int parallel_merge_worker_num = 16;
int pmem_log_worker_num = 8;

float range_sizes [] = {0.0001,  0.001, 0.01};
const int repeatn = 50000000;
uint64_t cacheline[8];

uint64_t strip_upsert_value(uint64_t v) {
    return v >> 1;
}

uint64_t make_upsert_value(uint64_t v) {
    return (v << 1);
}

// typedef uint64_t entry_type;
typedef entry_pair entry_type;

// entry_pair strip_upsert_value(entry_pair v) {
//     entry_pair result = entry_pair(v.first >> 1, v.second);
//     return result;
// }

// entry_pair make_upsert_value(entry_pair v) {
//     entry_pair result = entry_pair(v.first << 1, v.second);
//     return result;
// }


void concur_dptree_test(int nworkers, int n) {

    dptree::concur_dptree<uint64_t, entry_type> index;
    auto insert_func = [&](int start, int end) {
        for (int i = start; i < end; ++i) {
            // index.insert(keys[i], keys[i]);
            index.insert(keys[i], entry_type(keys[i], keys[i] + 1));
            // printf("inserted key %lu\n", keys[i]);
        }
    };
    
    measure([&]() -> unsigned {
        std::vector<std::thread> workers;
        int range_size = (n * 0.9) / nworkers;
        int start = 0;
        int end = range_size;
        for (;start < (n * 0.9);) {
            workers.emplace_back(std::thread(insert_func, start, end));
            start = end;
            end = std::min(end + range_size, (int)(n * 0.9));
        }
        std::for_each(workers.begin(), workers.end(), [](std::thread & t) { t.join(); });
        // printf("# merges: %d\n", index.get_merges());
        // printf("merge time: %f secs\n", index.get_merge_time());
        printf("real merge time: %f secs\n", index.get_real_merge_time());
        printf("merge wait time: %f secs\n", index.get_merge_wait_time());
        printf("merge work time: %f secs\n", index.get_merge_work_time());
        printf("inner nodes build time: %f secs\n", index.get_inner_node_build_time());
        printf("base tree flush time: %f secs\n", index.get_flushtime());

        return (n * 0.9);
    }, "concur-dptree-insert");
    while (index.is_merging());
    while (!(index.is_no_merge()));

    // shirley: construct pmlog
    double start_log_time = secs_now();
    index.construct_pmlog(false, true);
    double end_log_time = secs_now();
    printf("Logging Elapsed %lf seconds\n", end_log_time - start_log_time);

    // shirley: force merge
    double start_merge_time = secs_now();
    index.force_merge();
    double end_merge_time = secs_now();
    printf("Merge Elapsed %lf seconds\n", end_merge_time - start_merge_time);
    while (index.is_merging());
    while (!(index.is_no_merge()));
    end_merge_time = secs_now();
    printf("(Check) Merge Elapsed %lf seconds\n\n", end_merge_time - start_merge_time);

    // shirley: insert the rest 10% of keys
    measure([&]() -> unsigned {
        std::vector<std::thread> workers;
        int range_size = (n * 0.1) / nworkers;
        int start = n * 0.9;
        int end = start + range_size;
        for (;start < n;) {
            workers.emplace_back(std::thread(insert_func, start, end));
            start = end;
            end = std::min(end + range_size, (int)n);
        }
        std::for_each(workers.begin(), workers.end(), [](std::thread & t) { t.join(); });
        // printf("# merges: %d\n", index.get_merges());
        // printf("merge time: %f secs\n", index.get_merge_time());
        printf("real merge time: %f secs\n", index.get_real_merge_time());
        printf("merge wait time: %f secs\n", index.get_merge_wait_time());
        printf("merge work time: %f secs\n", index.get_merge_work_time());
        printf("inner nodes build time: %f secs\n", index.get_inner_node_build_time());
        printf("base tree flush time: %f secs\n", index.get_flushtime());

        return (n * 0.1);
    }, "concur-dptree-insert-extra");
    while (index.is_merging());
    while (!(index.is_no_merge()));

    // shirley: construct pmlog
    start_log_time = secs_now();
    index.construct_pmlog(false, true);
    end_log_time = secs_now();
    printf("Logging Elapsed %lf seconds\n", end_log_time - start_log_time);

    // shirley: force merge
    start_merge_time = secs_now();
    index.force_merge();
    end_merge_time = secs_now();
    printf("Merge Elapsed %lf seconds\n", end_merge_time - start_merge_time);
    while (index.is_merging());
    while (!(index.is_no_merge()));
    end_merge_time = secs_now();
    printf("(Check) Merge Elapsed %lf seconds\n\n", end_merge_time - start_merge_time);

    auto lookup_func = [&](int start, int end, std::atomic<int> & count) {
        int repeat = 1;
        int c = 0;
        repeat = repeatn / (end - start) / 2;
        if (repeat < 1)
            repeat = 1;
        for (uint64_t r = 0; r < repeat; ++r) {
            for (size_t i = start; i < end; i++) {
                uint64_t key = lookupKeys[i];
                // entry_type value = 0;
                entry_type value = entry_type(0, 0);
                bool res = index.lookup(key, value);
                assert(res);
                // assert(key == value);
                assert(key == (value).first);
                assert(key == (value).second - 1);
                // printf("looked up key %lu\n", key);
                c += 1;
            }
        }
        count += c;
    };
    
    measure([&]() -> unsigned {
        std::vector<std::thread> workers;
        int range_size = n / nworkers;
        int start = 0;
        int end = range_size;
        std::atomic<int> count(0);
        for (;start < n && workers.size() < nworkers;) {
            workers.emplace_back(std::thread(lookup_func, start, end, std::ref(count)));
            start = end;
            end = std::min(end + range_size, (int)n);
        }
        std::for_each(workers.begin(), workers.end(), [](std::thread & t) { t.join(); });
        printf("probes: %lu\n", index.get_probes());
        printf("avg probes per lookup: %f\n", index.get_probes() / (count.load() + 0.1));
        return count.load();
    }, "concur-dptree-search");

    

    // float scan_range_sizes[] = {0.0001, 0.001, 0.01};
    int scan_range_size = 0.00001 * keys.size();
    printf("scan range size: %d\n", scan_range_size);

    auto range_scan_func = [&](int start, int end, std::atomic<int> & count) {
        int repeat = 10;
        int c = 0;
        // repeat = repeatn / (end - start) / 2;
        // if (repeat < 1)
        //     repeat = 1;
        for (uint64_t r = 0; r < repeat; ++r) {
            for (size_t i = start; i < end; i++) {
                uint64_t startPos = (rand() % (end - start)) + start;
                uint64_t endPos = std::min(startPos + scan_range_size, (uint64_t)end);
                uint64_t keyStart = sortedKeys[startPos];
                // uint64_t keyEnd = sortedKeys[endPos];
                std::vector<entry_type> res;
                res.reserve(endPos - startPos);
                index.lookup_range(keyStart, endPos - startPos, res);
                assert(res.size() == endPos - startPos);
                // printf("scan size = %lu\n", endPos - startPos);
                for (int j = 0; j < res.size(); ++j) {
                    // if (res[j] != sortedKeys[startPos + j]) {
                    if ((res[j].first != sortedKeys[startPos + j]) || (res[j].second - 1 != sortedKeys[startPos + j])) {
                        assert(false);
                    }
                }
                i += (endPos - startPos);
                c++;
                // c+= (endPos - startPos);
            }
        }
        count += c;
    };

    measure([&]() -> unsigned {
        std::vector<std::thread> workers;
        int range_size = n / nworkers;
        int start = 0;
        int end = range_size;
        std::atomic<int> count(0);
        for (;start < n && workers.size() < nworkers;) {
            workers.emplace_back(std::thread(range_scan_func, start, end, std::ref(count)));
            start = end;
            end = std::min(end + range_size, (int)n);
        }
        std::for_each(workers.begin(), workers.end(), [](std::thread & t) { t.join(); });
        return count.load();
    }, "concur-dptree-scan-" + std::to_string(scan_range_size));

}


int main(int argc, char const *argv[]) {
    int n = 10000000;
    bool sparseKey = false;
    int nworkers = 8; // shirley: default was 1
    if (argc > 1)
        n = atoi(argv[1]);
    if (argc > 2)
        nworkers = atoi(argv[2]);
    if (argc > 3) {
        parallel_merge_worker_num = atoi(argv[3]);
        pmem_log_worker_num = atoi(argv[3]);
    }
    else {
        parallel_merge_worker_num = nworkers;
        pmem_log_worker_num = nworkers;
    }
    if (argc > 4)
        sparseKey = atoi(argv[4]);
    if (argc > 5)
        write_latency_in_ns = atoi(argv[5]);
    printf("nworkers: %d, parallel_merge_worker_num %d, pmem_log_worker_num %d\n", nworkers, parallel_merge_worker_num, pmem_log_worker_num);
    prepareKeys(n, "", sparseKey, false);
    concur_dptree_test(nworkers, n);
    return 0;
}