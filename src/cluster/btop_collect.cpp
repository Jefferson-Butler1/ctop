/* Copyright 2021 Aristocratos (jakob@qvantnet.com)

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

	   http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

indent = tab
tab-size = 4
*/

//* ctop cluster backend: the same collectors as the per-OS backends, fed from every node's node-exporter
//* and the Kubernetes API instead of the local machine. The cluster reads as one big machine: all cores
//* appended, memory summed, one disk per node filesystem, one net "interface" per node plus a cluster
//* total, and pods in place of processes (grouped under their node in tree mode).

#include <chrono>
#include <cmath>
#include <cstdio>
#include <ctime>
#include <future>
#include <iomanip>
#include <mutex>
#include <numeric>
#include <ranges>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <sys/time.h>

#include <fmt/format.h>

#include "../btop_config.hpp"
#include "../btop_log.hpp"
#include "../btop_shared.hpp"
#include "../btop_tools.hpp"

using std::clamp;
using std::cmp_greater;
using std::cmp_less;
using std::max;
using std::min;
using std::round;
namespace rng = std::ranges;
using namespace Tools;

namespace Cluster {
	constexpr int exporter_port = 9100;

	//? Modes of node_cpu_seconds_total, named as btop's cpu_percent fields
	const array<string, 8> cpu_modes = {"user", "nice", "system", "idle", "iowait", "irq", "softirq", "steal"};

	struct Filesystem {
		string device, mount, fstype;
		double size{}, avail{};
	};

	//* One scrape of a node's counters
	struct Sample {
		bool ok{};
		uint64_t at{};
		vector<array<double, 8>> cpu; // seconds per mode, indexed by core
		std::unordered_map<string, double> mem; // node_memory_<key>_bytes
		array<double, 3> load{};
		double rx{}, tx{}, boot{};
		std::unordered_map<string, array<double, 3>> disk; // device -> read bytes, written bytes, io seconds
		vector<Filesystem> fs;
	};

	struct Node {
		string name, addr;
		size_t cores{};
		bool up{};
		Sample cur, prev;
	};

	struct Pod {
		string ns, name, node, phase, images;
		size_t containers{};
		double cpu{}, mem{}; // cores, bytes
		time_t start{};
	};

	//? The collectors read `nodes` on the runner thread; the scrape thread fills `scraped`, and refresh() hands it over
	vector<Node> nodes, scraped;
	std::mutex scrape_mutex;
	bool fresh = false;

	std::mutex pods_mutex;
	vector<Pod> pods;

	string run(const string& cmd) {
		string out;
		if (FILE* f = popen(cmd.c_str(), "r")) {
			array<char, 65536> buf;
			size_t n;
			while ((n = fread(buf.data(), 1, buf.size(), f)) > 0) out.append(buf.data(), n);
			pclose(f);
		}
		return out;
	}

	string_view label(string_view labels, string_view key) {
		const string needle = fmt::format("{}=\"", key);
		auto pos = labels.find(needle);
		if (pos == string_view::npos) return {};
		pos += needle.size();
		return labels.substr(pos, labels.find('"', pos) - pos);
	}

	//? Physical NICs only; pod veths, flannel, tailscale etc. would double count
	bool is_nic(string_view dev) {
		return dev.starts_with("en") or dev.starts_with("eth") or dev.starts_with("wl") or dev.starts_with("bond");
	}

	//? node-exporter reports IO per whole disk, so map a partition to its disk: /dev/nvme0n1p3 -> nvme0n1, /dev/sda5 -> sda
	string whole_disk(string_view device) {
		if (not device.starts_with("/dev/")) return {};
		string dev{device.substr(5)};
		if (dev.starts_with("nvme") or dev.starts_with("mmcblk")) {
			if (auto p = dev.rfind('p'); p != string::npos and p > 0 and isdigit(dev[p - 1])) dev.resize(p);
		}
		else while (not dev.empty() and isdigit(dev.back())) dev.pop_back();
		return dev;
	}

	Sample parse(string_view text) {
		Sample s{.at = time_ms()};
		while (not text.empty()) {
			const auto eol = text.find('\n');
			const auto line = text.substr(0, eol);
			text = (eol == string_view::npos ? string_view{} : text.substr(eol + 1));
			if (not line.starts_with("node_")) continue;

			const auto space = line.rfind(' ');
			if (space == string_view::npos) continue;
			const double val = std::strtod(string(line.substr(space + 1)).c_str(), nullptr);
			const auto head = line.substr(0, space);
			const auto brace = head.find('{');
			const auto name = head.substr(0, brace);
			const auto labels = (brace == string_view::npos ? string_view{} : head.substr(brace));

			if (name == "node_cpu_seconds_total") {
				const size_t core = std::stoul(string(label(labels, "cpu")));
				if (s.cpu.size() <= core) s.cpu.resize(core + 1);
				const auto mode = label(labels, "mode");
				for (size_t i = 0; i < cpu_modes.size(); i++)
					if (cpu_modes[i] == mode) s.cpu[core][i] = val;
			}
			else if (name.starts_with("node_memory_") and name.ends_with("_bytes"))
				s.mem[string(name.substr(12, name.size() - 12 - 6))] = val;
			else if (name == "node_load1") s.load[0] = val;
			else if (name == "node_load5") s.load[1] = val;
			else if (name == "node_load15") s.load[2] = val;
			else if (name == "node_boot_time_seconds") s.boot = val;
			else if (name == "node_network_receive_bytes_total" and is_nic(label(labels, "device"))) s.rx += val;
			else if (name == "node_network_transmit_bytes_total" and is_nic(label(labels, "device"))) s.tx += val;
			else if (name == "node_disk_read_bytes_total") s.disk[string(label(labels, "device"))][0] = val;
			else if (name == "node_disk_written_bytes_total") s.disk[string(label(labels, "device"))][1] = val;
			else if (name == "node_disk_io_time_seconds_total") s.disk[string(label(labels, "device"))][2] = val;
			else if (name == "node_filesystem_size_bytes" or name == "node_filesystem_avail_bytes") {
				const auto fstype = label(labels, "fstype");
				if (not is_in(fstype, "ext4", "ext3", "xfs", "btrfs", "zfs", "f2fs")) continue;
				const string mount{label(labels, "mountpoint")};
				auto fs = rng::find(s.fs, mount, &Filesystem::mount);
				if (fs == s.fs.end()) {
					s.fs.push_back({string(label(labels, "device")), mount, string(fstype)});
					fs = s.fs.end() - 1;
				}
				(name.ends_with("size_bytes") ? fs->size : fs->avail) = val;
			}
		}

		//? Bind mounts repeat a partition under several mountpoints; keep the shortest mountpoint per device
		rng::sort(s.fs, rng::less{}, [](const Filesystem& f) { return f.mount.size(); });
		std::unordered_set<string> seen;
		std::erase_if(s.fs, [&](const Filesystem& f) { return not seen.insert(f.device).second; });
		rng::sort(s.fs, rng::less{}, &Filesystem::mount);

		s.ok = not s.cpu.empty();
		return s;
	}

	//? Only the collectors ctop reads; the full exporter output is several times larger and slower to produce
	constexpr auto collectors = "collect[]=cpu&collect[]=meminfo&collect[]=loadavg&collect[]=netdev"
		"&collect[]=diskstats&collect[]=filesystem&collect[]=stat";

	//* Scrape every node in parallel
	void scrape(vector<Node>& into) {
		vector<std::future<string>> scrapes;
		for (const auto& node : into)
			scrapes.push_back(std::async(std::launch::async, run,
				fmt::format("curl -sg --max-time 2 'http://{}:{}/metrics?{}' 2>/dev/null", node.addr, exporter_port, collectors)));

		for (size_t i = 0; i < into.size(); i++) {
			auto& node = into[i];
			auto sample = parse(scrapes[i].get());
			node.up = sample.ok;
			//? A down node keeps its last sample, so cluster totals don't dip and spike around an outage
			if (node.up) {
				node.prev = std::move(node.cur);
				node.cur = std::move(sample);
			}
		}
	}

	//? A scrape takes up to ~200ms, so it runs off the runner thread and drawing and input never wait on the network
	void scrape_loop() {
		while (not Global::quitting) {
			const auto start = time_ms();
			vector<Node> work;
			{
				std::lock_guard lock(scrape_mutex);
				work = scraped;
			}
			scrape(work);
			{
				std::lock_guard lock(scrape_mutex);
				scraped = std::move(work);
				fresh = true;
			}
			while (not Global::quitting and time_ms() - start < (uint64_t)Config::getI("update_ms"))
				std::this_thread::sleep_for(std::chrono::milliseconds(20));
		}
	}

	//* Pick up the latest scrape, if there's a new one
	void refresh() {
		std::lock_guard lock(scrape_mutex);
		if (fresh) {
			nodes = scraped;
			fresh = false;
		}
	}

	//? Per-second rate of a counter between a node's last two samples
	double rate(const Node& node, double cur, double prev) {
		if (not node.up or not node.prev.ok or node.cur.at <= node.prev.at) return 0;
		return max(0.0, cur - prev) / ((node.cur.at - node.prev.at) / 1000.0);
	}

	//? Busy fraction of a node's cpu between its last two samples (iowait counts as idle, like btop on Linux)
	double busy(const Node& node, size_t core_begin, size_t core_end) {
		if (not node.up or not node.prev.ok) return 0;
		double total = 0, idle = 0;
		for (size_t c = core_begin; c < min({core_end, node.cur.cpu.size(), node.prev.cpu.size()}); c++) {
			for (size_t m = 0; m < cpu_modes.size(); m++) {
				const double d = max(0.0, node.cur.cpu[c][m] - node.prev.cpu[c][m]);
				total += d;
				if (m == 3 or m == 4) idle += d;
			}
		}
		return total > 0 ? (total - idle) / total : 0;
	}

	double quantity(string_view q) {
		for (const auto& [suffix, mult] : {std::pair{"Ki"sv, 1024.0}, {"Mi"sv, 1048576.0}, {"Gi"sv, 1073741824.0}, {"m"sv, 0.001}})
			if (q.ends_with(suffix)) return std::strtod(string(q.substr(0, q.size() - suffix.size())).c_str(), nullptr) * mult;
		return std::strtod(string(q).c_str(), nullptr);
	}

	vector<Pod> fetch_pods() {
		vector<Pod> out;
		std::unordered_map<string, size_t> index;
		std::istringstream list(run("kubectl get pods -A --no-headers -o custom-columns="
			"NS:.metadata.namespace,NAME:.metadata.name,NODE:.spec.nodeName,PHASE:.status.phase,"
			"START:.status.startTime,IMAGES:.spec.containers[*].image 2>/dev/null"));
		for (string line; std::getline(list, line);) {
			std::istringstream fields(line);
			Pod pod;
			string start;
			if (not (fields >> pod.ns >> pod.name >> pod.node >> pod.phase >> start >> pod.images)) continue;
			if (is_in(pod.phase, "Succeeded", "Failed")) continue;
			pod.containers = rng::count(pod.images, ',') + 1;
			std::tm tm{};
			std::istringstream(start) >> std::get_time(&tm, "%Y-%m-%dT%H:%M:%SZ");
			pod.start = timegm(&tm);
			index[pod.ns + "/" + pod.name] = out.size();
			out.push_back(std::move(pod));
		}

		std::istringstream top(run("kubectl top pods -A --no-headers 2>/dev/null"));
		for (string line; std::getline(top, line);) {
			std::istringstream fields(line);
			string ns, name, cpu, mem;
			if (not (fields >> ns >> name >> cpu >> mem)) continue;
			if (auto it = index.find(ns + "/" + name); it != index.end()) {
				out[it->second].cpu = quantity(cpu);
				out[it->second].mem = quantity(mem);
			}
		}
		return out;
	}

	//? metrics-server only refreshes every ~15s and kubectl is slow, so pods are fetched off the runner thread
	void pods_loop() {
		while (not Global::quitting) {
			auto fresh = fetch_pods();
			{
				std::lock_guard lock(pods_mutex);
				pods = std::move(fresh);
			}
			for (int i = 0; i < 20 and not Global::quitting; i++) std::this_thread::sleep_for(std::chrono::milliseconds(250));
		}
	}
} // namespace Cluster

using Cluster::nodes;

namespace Cpu {
	extern cpu_info current_cpu;
}

namespace Shared {
	uint64_t totalMem;
	long coreCount, page_size = 4096, clk_tck = 100;

	void init() {
		std::istringstream list(Cluster::run(R"cmd(kubectl get nodes -o jsonpath='{range .items[*]}{.metadata.name}{" "}{.status.addresses[?(@.type=="InternalIP")].address}{"\n"}{end}' 2>/dev/null)cmd"));
		for (string line; std::getline(list, line);) {
			std::istringstream fields(line);
			Cluster::Node node;
			if (fields >> node.name >> node.addr) nodes.push_back(std::move(node));
		}
		if (nodes.empty()) {
			Global::exit_error_msg = "ctop: couldn't list cluster nodes with kubectl (check your kubeconfig)";
			clean_quit(1);
		}

		//? Core count is fixed at startup, like btop: nodes unreachable now contribute no cores
		Cluster::scrape(nodes);
		coreCount = 0;
		for (auto& node : nodes) {
			node.cores = node.cur.cpu.size();
			for (size_t i = 0; i < node.cores; i++) Cpu::core_labels.push_back(node.name.substr(0, 1) + to_string(i));
			coreCount += node.cores;
		}
		coreCount = max(coreCount, 1l);

		Cpu::current_cpu.core_percent.insert(Cpu::current_cpu.core_percent.begin(), coreCount, {});
		Cpu::current_cpu.temp.insert(Cpu::current_cpu.temp.begin(), coreCount + 1, {});
		Cpu::collect();
		for (auto& [field, vec] : Cpu::current_cpu.cpu_percent) {
			if (not vec.empty() and not v_contains(Cpu::available_fields, field)) Cpu::available_fields.push_back(field);
		}
		Cpu::cpuName = fmt::format("Cluster: {} nodes", nodes.size());

		Mem::collect();

		Cluster::scraped = nodes;
		std::thread(Cluster::scrape_loop).detach();
		std::thread(Cluster::pods_loop).detach();
	}
} // namespace Shared

namespace Cpu {
	vector<string> available_fields = {"Auto", "total"};
	vector<string> available_sensors = {"Auto"};
	cpu_info current_cpu;
	bool got_sensors = false, cpu_temp_only = false, supports_watts = false, has_battery = false;
	string cpuName, cpuHz;
	tuple<int, float, long, string> current_bat;
	std::unordered_map<int, int> core_mapping;

	string get_cpuHz() { return ""; }

	auto get_core_mapping() -> std::unordered_map<int, int> { return {}; }

	auto get_battery() -> tuple<int, float, long, string> { return {0, 0, 0, ""}; }

	auto collect(bool no_update) -> cpu_info& {
		if (Runner::stopping or (no_update and not current_cpu.cpu_percent.at("total").empty())) return current_cpu;
		Cluster::refresh();
		auto& cpu = current_cpu;

		cpu.load_avg = {};
		array<double, 8> mode_totals{};
		size_t core = 0;
		for (const auto& node : nodes) {
			for (size_t i = 0; i < node.cores and cmp_less(core, Shared::coreCount); i++, core++) {
				cpu.core_percent.at(core).push_back(round(Cluster::busy(node, i, i + 1) * 100));
				if (cpu.core_percent.at(core).size() > 40) cpu.core_percent.at(core).pop_front();
				if (node.up and node.prev.ok and i < node.cur.cpu.size() and i < node.prev.cpu.size())
					for (size_t m = 0; m < mode_totals.size(); m++)
						mode_totals[m] += max(0.0, node.cur.cpu[i][m] - node.prev.cpu[i][m]);
			}
			if (node.up)
				for (size_t k = 0; k < 3; k++) cpu.load_avg[k] += node.cur.load[k];
		}

		const double total = std::accumulate(mode_totals.begin(), mode_totals.end(), 0.0);
		auto push = [&](const string& field, double percent) {
			auto& graph = cpu.cpu_percent.at(field);
			graph.push_back(clamp((long long)round(percent), 0ll, 100ll));
			while (cmp_greater(graph.size(), width * 2)) graph.pop_front();
		};
		for (size_t m = 0; m < Cluster::cpu_modes.size(); m++)
			push(Cluster::cpu_modes[m], total > 0 ? mode_totals[m] * 100 / total : 0);
		push("total", total > 0 ? (total - mode_totals[3] - mode_totals[4]) * 100 / total : 0);

		return cpu;
	}
} // namespace Cpu

namespace Mem {
	bool has_swap = false;
	int disk_ios = 0;
	mem_info current_mem{};

	uint64_t get_totalMem() {
		return Shared::totalMem;
	}

	auto collect(bool no_update) -> mem_info& {
		if (Runner::stopping or (no_update and not current_mem.percent.at("used").empty())) return current_mem;
		Cluster::refresh();
		auto& mem = current_mem;
		auto show_swap = Config::getB("show_swap");
		auto swap_disk = Config::getB("swap_disk");

		double total = 0, available = 0, free = 0, cached = 0, swap_total = 0, swap_free = 0;
		for (const auto& node : nodes) {
			if (not node.up) continue;
			auto get = [&](const string& key) { auto it = node.cur.mem.find(key); return it == node.cur.mem.end() ? 0.0 : it->second; };
			total += get("MemTotal");
			available += get("MemAvailable");
			free += get("MemFree");
			cached += get("Cached") + get("Buffers");
			swap_total += get("SwapTotal");
			swap_free += get("SwapFree");
		}
		Shared::totalMem = max(total, 1.0);
		mem.stats.at("available") = available;
		mem.stats.at("used") = total - available;
		mem.stats.at("free") = free;
		mem.stats.at("cached") = cached;
		mem.stats.at("swap_total") = swap_total;
		mem.stats.at("swap_free") = swap_free;
		mem.stats.at("swap_used") = swap_total - swap_free;

		if (show_swap and mem.stats.at("swap_total") > 0) {
			for (const auto& name : swap_names) {
				mem.percent.at(name).push_back(round((double)mem.stats.at(name) * 100 / mem.stats.at("swap_total")));
				while (cmp_greater(mem.percent.at(name).size(), width * 2)) mem.percent.at(name).pop_front();
			}
			has_swap = true;
		}
		else
			has_swap = false;

		for (const auto& name : mem_names) {
			mem.percent.at(name).push_back(round((double)mem.stats.at(name) * 100 / Shared::totalMem));
			while (cmp_greater(mem.percent.at(name).size(), width * 2)) mem.percent.at(name).pop_front();
		}

		if (Config::getB("show_disks")) {
			auto& disks = mem.disks;
			auto& disks_filter = Config::getS("disks_filter");
			bool filter_exclude = false;
			vector<string> filter;
			if (not disks_filter.empty()) {
				filter = ssplit(disks_filter);
				if (filter.at(0).starts_with("exclude=")) {
					filter_exclude = true;
					filter.at(0) = filter.at(0).substr(8);
				}
			}

			auto push = [](deque<long long>& graph, double val) {
				graph.push_back(round(val));
				while (cmp_greater(graph.size(), width * 2)) graph.pop_front();
			};

			//? One disk per node filesystem, keyed "node:/mount" and shown as just "node" for its root
			disk_ios = 0;
			vector<string> found;
			for (const auto& node : nodes) {
				if (not node.up) continue;
				for (const auto& fs : node.cur.fs) {
					const string key = node.name + ":" + fs.mount;
					if (not filter.empty()) {
						bool match = v_contains(filter, key);
						if ((filter_exclude and match) or (not filter_exclude and not match)) continue;
					}
					found.push_back(key);
					if (not disks.contains(key)) {
						disks[key] = {fs.device, fs.mount == "/" ? node.name : key, fs.fstype};
						redraw = true;
					}
					auto& disk = disks.at(key);
					disk.total = fs.size;
					disk.free = fs.avail;
					disk.used = disk.total - disk.free;
					disk.used_percent = disk.total > 0 ? round((double)disk.used * 100 / disk.total) : 0;
					disk.free_percent = 100 - disk.used_percent;

					const auto dev = Cluster::whole_disk(fs.device);
					const auto cur = node.cur.disk.find(dev), prev = node.prev.disk.find(dev);
					if (cur == node.cur.disk.end() or prev == node.prev.disk.end()) continue;
					disk_ios++;
					push(disk.io_read, Cluster::rate(node, cur->second[0], prev->second[0]));
					push(disk.io_write, Cluster::rate(node, cur->second[1], prev->second[1]));
					push(disk.io_activity, clamp(Cluster::rate(node, cur->second[2], prev->second[2]) * 100, 0.0, 100.0));
				}
			}

			if (swap_disk and has_swap) found.push_back("swap");
			std::erase_if(disks, [&](const auto& d) { return not v_contains(found, d.first); });

			mem.disks_order = found;
			if (swap_disk and has_swap) {
				if (not disks.contains("swap")) disks["swap"] = {"", "swap"};
				auto& swap = disks.at("swap");
				swap.total = mem.stats.at("swap_total");
				swap.used = mem.stats.at("swap_used");
				swap.free = mem.stats.at("swap_free");
				swap.used_percent = mem.percent.at("swap_used").back();
				swap.free_percent = mem.percent.at("swap_free").back();
			}
		}
		return mem;
	}
} // namespace Mem

namespace Net {
	std::unordered_map<string, net_info> current_net;
	net_info empty_net = {};
	vector<string> interfaces;
	string selected_iface;
	std::unordered_map<string, uint64_t> graph_max = {{"download", {}}, {"upload", {}}};
	std::unordered_map<string, array<int, 2>> max_count = {{"download", {}}, {"upload", {}}};
	bool rescale = true;

	auto collect(bool no_update) -> net_info& {
		auto& net = current_net;
		auto& config_iface = Config::getS("net_iface");
		auto net_sync = Config::getB("net_sync");
		auto net_auto = Config::getB("net_auto");

		if (not no_update) {
			Cluster::refresh();

			//? Each node is an "interface" (its physical NICs summed); "cluster" is all of them
			interfaces = {"cluster"};
			auto& cluster = net["cluster"];
			cluster.connected = true;
			uint64_t cluster_counter[2] = {}, cluster_speed[2] = {};

			for (const auto& node : nodes) {
				interfaces.push_back(node.name);
				auto& iface = net[node.name];
				iface.connected = node.up;
				iface.ipv4 = node.addr;
				for (int d = 0; const string dir : {"download", "upload"}) {
					auto& stat = iface.stat.at(dir);
					const uint64_t val = (d == 0 ? node.cur.rx : node.cur.tx);
					//? Counters restart when a node reboots; carry what it had so totals keep counting up
					if (val < stat.last) {
						stat.rollover += stat.last;
						stat.last = 0;
					}
					stat.speed = round(d == 0 ? Cluster::rate(node, node.cur.rx, node.prev.rx) : Cluster::rate(node, node.cur.tx, node.prev.tx));
					if (stat.speed > stat.top) stat.top = stat.speed;
					if (stat.offset > val + stat.rollover) stat.offset = 0;
					stat.total = (val + stat.rollover) - stat.offset;
					stat.last = val;
					cluster_counter[d] += val + stat.rollover;
					cluster_speed[d] += stat.speed;
					d++;
				}
			}

			for (int d = 0; const string dir : {"download", "upload"}) {
				auto& stat = cluster.stat.at(dir);
				stat.speed = cluster_speed[d];
				if (stat.speed > stat.top) stat.top = stat.speed;
				if (stat.offset > cluster_counter[d]) stat.offset = 0;
				stat.total = cluster_counter[d] - stat.offset;
				stat.last = cluster_counter[d];
				d++;
			}

			for (const auto& iface : interfaces) {
				for (const string dir : {"download", "upload"}) {
					const auto speed = net.at(iface).stat.at(dir).speed;
					auto& bandwidth = net.at(iface).bandwidth.at(dir);
					bandwidth.push_back(speed);
					while (cmp_greater(bandwidth.size(), width * 2)) bandwidth.pop_front();

					//? Set counters for auto scaling
					if (net_auto and selected_iface == iface) {
						if (speed > graph_max[dir]) {
							++max_count[dir][0];
							if (max_count[dir][1] > 0) --max_count[dir][1];
						}
						else if (graph_max[dir] > 10 << 10 and speed < graph_max[dir] / 10) {
							++max_count[dir][1];
							if (max_count[dir][0] > 0) --max_count[dir][0];
						}
					}
				}
			}
		}

		if (net.empty()) return empty_net;

		if (selected_iface.empty() or not v_contains(interfaces, selected_iface)) {
			max_count["download"][0] = max_count["download"][1] = max_count["upload"][0] = max_count["upload"][1] = 0;
			redraw = true;
			if (net_auto) rescale = true;
			selected_iface = (not config_iface.empty() and v_contains(interfaces, config_iface)) ? config_iface : "cluster";
		}

		//? Calculate max scale for graphs if needed
		if (net_auto) {
			bool sync = false;
			for (const auto& dir : {"download", "upload"}) {
				for (const auto& sel : {0, 1}) {
					if (rescale or max_count[dir][sel] >= 5) {
						const long long avg_speed = (net[selected_iface].bandwidth[dir].size() > 5
							? std::accumulate(net.at(selected_iface).bandwidth.at(dir).rbegin(), net.at(selected_iface).bandwidth.at(dir).rbegin() + 5, 0ll) / 5
							: net[selected_iface].stat[dir].speed);
						graph_max[dir] = max(uint64_t(avg_speed * (sel == 0 ? 1.3 : 3.0)), (uint64_t)10 << 10);
						max_count[dir][0] = max_count[dir][1] = 0;
						redraw = true;
						if (net_sync) sync = true;
						break;
					}
				}
				//? Sync download/upload graphs if enabled
				if (sync) {
					const auto other = (string(dir) == "upload" ? "download" : "upload");
					graph_max[other] = graph_max[dir];
					max_count[other][0] = max_count[other][1] = 0;
					break;
				}
			}
		}

		rescale = false;
		return net.at(selected_iface);
	}
} // namespace Net

namespace Proc {
	vector<proc_info> current_procs;
	string current_sort;
	string current_filter;
	bool current_rev = false;
	bool is_tree_mode;

	int collapse = -1, expand = -1, toggle_children = -1, collapse_all = -1;
	atomic<int> numpids = 0;
	int filter_found = 0;

	detail_container detailed;
	static std::unordered_set<size_t> dead_procs;

	//? Pids are synthetic: nodes are 1..N so pods can name their node as parent, pods count up from 1000
	std::unordered_map<string, size_t> pod_pids;
	size_t next_pod_pid = 1000;

	string get_status(char s) {
		switch (s) {
			case 'R': return "Running";
			case 'P': return "Pending";
			case 'D': return "Down";
			default: return "Unknown";
		}
	}

	//* Get detailed info for selected pod or node
	void _collect_details(const size_t pid, vector<proc_info>& procs) {
		if (pid != detailed.last_pid) {
			detailed = {};
			detailed.last_pid = pid;
			detailed.skip_smaps = true;
		}

		auto p_info = rng::find(procs, pid, &proc_info::pid);
		if (p_info == procs.end()) return;
		detailed.entry = *p_info;

		//? Update cpu percent deque for process cpu graph
		if (not Config::getB("proc_per_core")) detailed.entry.cpu_p *= Shared::coreCount;
		detailed.cpu_percent.push_back(clamp((long long)round(detailed.entry.cpu_p), 0ll, 100ll));
		while (cmp_greater(detailed.cpu_percent.size(), width)) detailed.cpu_percent.pop_front();

		//? cpu_s holds the start time in microseconds since epoch
		detailed.elapsed = sec_to_dhms(time(nullptr) - (detailed.entry.cpu_s / 1'000'000));
		if (detailed.elapsed.size() > 8) detailed.elapsed.resize(detailed.elapsed.size() - 3);

		if (detailed.parent.empty()) {
			auto p_entry = rng::find(procs, detailed.entry.ppid, &proc_info::pid);
			if (p_entry != procs.end()) detailed.parent = p_entry->name;
		}

		detailed.status = get_status(detailed.entry.state);

		detailed.mem_bytes.push_back(detailed.entry.mem);
		detailed.memory = floating_humanizer(detailed.entry.mem);

		if (detailed.first_mem == -1 or detailed.first_mem < detailed.mem_bytes.back() / 2 or detailed.first_mem > detailed.mem_bytes.back() * 4) {
			detailed.first_mem = min((uint64_t)detailed.mem_bytes.back() * 2, Mem::get_totalMem());
			redraw = true;
		}

		while (cmp_greater(detailed.mem_bytes.size(), width)) detailed.mem_bytes.pop_front();
	}

	//* Collects and sorts pods (and their nodes) as processes
	auto collect(bool no_update) -> vector<proc_info>& {
		const auto& sorting = Config::getS("proc_sorting");
		auto reverse = Config::getB("proc_reversed");
		const auto& filter = Config::getS("proc_filter");
		auto per_core = Config::getB("proc_per_core");
		auto tree = Config::getB("proc_tree");
		auto show_detailed = Config::getB("show_detailed");
		const auto pause_proc_list = Config::getB("pause_proc_list");
		const size_t detailed_pid = Config::getI("detailed_pid");
		bool should_filter = current_filter != filter;
		if (should_filter) current_filter = filter;
		bool sorted_change = (sorting != current_sort or reverse != current_rev or should_filter);
		bool tree_mode_change = tree != is_tree_mode;
		if (sorted_change) {
			current_sort = sorting;
			current_rev = reverse;
		}
		if (tree_mode_change) is_tree_mode = tree;

		bool got_detailed = false;

		static vector<size_t> found;

		//* Use pids from last update if only changing filter, sorting or tree options
		if (no_update and not current_procs.empty()) {
			if (show_detailed and detailed_pid != detailed.last_pid) _collect_details(detailed_pid, current_procs);
		}
		else {
			//* ---------------------------------------------Collection start----------------------------------------------
			should_filter = true;
			found.clear();
			Cluster::refresh();

			vector<Cluster::Pod> pods;
			{
				std::lock_guard lock(Cluster::pods_mutex);
				pods = Cluster::pods;
			}

			//? cpu_p is percent of the whole cluster, or of one core with proc_per_core
			const double scale = per_core ? 100.0 : 100.0 / Shared::coreCount;

			//? Find or add the entry for pid, nullptr if it should be skipped
			auto entry = [&](size_t pid) -> proc_info* {
				auto find_old = rng::find(current_procs, pid, &proc_info::pid);
				if (find_old == current_procs.end()) {
					if (pause_proc_list) return nullptr;
					current_procs.push_back({pid});
					find_old = current_procs.end() - 1;
				}
				else if (dead_procs.contains(pid)) return nullptr;
				found.push_back(pid);
				if (show_detailed and pid == detailed_pid) got_detailed = true;
				return &*find_old;
			};

			std::unordered_map<string, size_t> node_pid, node_pods;
			for (const auto& pod : pods) node_pods[pod.node]++;

			for (size_t i = 0; i < nodes.size(); i++) {
				const auto& node = nodes[i];
				node_pid[node.name] = i + 1;
				if (auto* p = entry(i + 1)) {
					auto get = [&](const string& key) { auto it = node.cur.mem.find(key); return it == node.cur.mem.end() ? 0.0 : it->second; };
					p->name = node.name;
					p->cmd = fmt::format("node {} ({} cores)", node.addr, node.cores);
					p->user = "node";
					p->threads = node_pods[node.name];
					p->mem = get("MemTotal") - get("MemAvailable");
					p->cpu_p = Cluster::busy(node, 0, node.cores) * node.cores * scale;
					p->cpu_c = p->cpu_p;
					p->state = node.up ? 'R' : 'D';
					p->cpu_s = node.cur.boot * 1'000'000;
					p->ppid = 0;
				}
			}

			for (const auto& pod : pods) {
				auto [it, added] = pod_pids.try_emplace(pod.ns + "/" + pod.name, next_pod_pid);
				if (added) next_pod_pid++;
				if (auto* p = entry(it->second)) {
					p->name = pod.name;
					p->cmd = pod.images;
					p->user = pod.ns;
					p->threads = pod.containers;
					p->mem = pod.mem;
					p->cpu_p = pod.cpu * scale;
					p->cpu_c = p->cpu_p;
					p->state = pod.phase == "Running" ? 'R' : 'P';
					p->cpu_s = pod.start * 1'000'000;
					p->ppid = node_pid.contains(pod.node) ? node_pid.at(pod.node) : 0;
				}
			}

			//? Clear dead processes from current_procs if not paused
			if (not pause_proc_list) {
				auto eraser = rng::remove_if(current_procs, [&](const auto& element) { return not v_contains(found, element.pid); });
				current_procs.erase(eraser.begin(), eraser.end());
				if (!dead_procs.empty()) dead_procs.clear();
			}
			//? Set correct state of dead processes if paused
			else {
				const bool keep_dead_proc_usage = Config::getB("keep_dead_proc_usage");
				for (auto& r : current_procs) {
					if (rng::find(found, r.pid) == found.end()) {
						if (r.state != 'X') r.death_time = time(nullptr) - (r.cpu_s / 1'000'000);
						r.state = 'X';
						dead_procs.emplace(r.pid);
						//? Reset cpu usage for dead processes if paused and option is set
						if (!keep_dead_proc_usage) {
							r.cpu_p = 0.0;
							r.mem = 0;
						}
					}
				}
			}

			//? Update the details info box for process if active
			if (show_detailed and got_detailed) {
				_collect_details(detailed_pid, current_procs);
			}
			else if (show_detailed and not got_detailed and detailed.status != "Dead") {
				detailed.status = "Dead";
				redraw = true;
			}
		}

		//* ---------------------------------------------Collection done-----------------------------------------------

		//* Match filter if defined
		if (should_filter) {
			filter_found = 0;
			for (auto& p : current_procs) {
				if (not tree and not filter.empty()) {
					if (!matches_filter(p, filter)) {
						p.filtered = true;
						filter_found++;
					}
					else {
						p.filtered = false;
					}
				}
				else {
					p.filtered = false;
				}
			}
		}

		//* Sort processes
		if ((sorted_change or tree_mode_change) or (not no_update and not pause_proc_list)) {
			proc_sorter(current_procs, sorting, reverse, tree);
		}

		//* Generate tree view if enabled
		if (tree and not current_procs.empty() and (not no_update or should_filter or sorted_change)) {
			bool locate_selection = false;

			if (toggle_children != -1) {
				auto collapser = rng::find(current_procs, toggle_children, &proc_info::pid);
				if (collapser != current_procs.end()) {
					for (auto& p : current_procs) {
						if (p.ppid == collapser->pid) {
							auto child = rng::find(current_procs, p.pid, &proc_info::pid);
							if (child != current_procs.end()) {
								child->collapsed = not child->collapsed;
							}
						}
					}
					if (Config::ints.at("proc_selected") > 0) locate_selection = true;
				}
				toggle_children = -1;
			}

			if (auto find_pid = (collapse != -1 ? collapse : expand); find_pid != -1) {
				auto collapser = rng::find(current_procs, find_pid, &proc_info::pid);
				if (collapser != current_procs.end()) {
					if (collapse == expand) {
						collapser->collapsed = not collapser->collapsed;
					}
					else if (collapse > -1) {
						collapser->collapsed = true;
					}
					else if (expand > -1) {
						collapser->collapsed = false;
					}
					if (Config::ints.at("proc_selected") > 0) locate_selection = true;
				}
				collapse = expand = -1;
			}

			if (collapse_all != -1) {
				toggle_tree_collapse(current_procs);
				collapse_all = -1;
				if (Config::ints.at("proc_selected") > 0) locate_selection = true;
			}

			if (should_filter or not filter.empty()) filter_found = 0;

			vector<tree_proc> tree_procs;
			tree_procs.reserve(current_procs.size());

			if (!pause_proc_list) {
				for (auto& p : current_procs) {
					if (not v_contains(found, p.ppid)) p.ppid = 0;
				}
			}

			//? Stable sort to retain selected sorting among processes with the same parent
			rng::stable_sort(current_procs, rng::less{}, &proc_info::ppid);

			//? Auto-collapse processes with many children when entering tree mode
			_auto_collapse_oversized(current_procs, tree_mode_change);

			//? Start recursive iteration over processes with the lowest shared parent pids
			for (auto& p : rng::equal_range(current_procs, current_procs.at(0).ppid, rng::less{}, &proc_info::ppid)) {
				_tree_gen(p, current_procs, tree_procs, 0, false, filter, false, no_update, should_filter);
			}

			//? Recursive sort over tree structure to account for collapsed processes in the tree
			int index = 0;
			tree_sort(tree_procs, sorting, reverse, (pause_proc_list and not (sorted_change or tree_mode_change)), index, current_procs.size());

			//? Recursive construction of ASCII tree prefixes.
			for (auto t = tree_procs.begin(); t != tree_procs.end(); ++t) {
				_collect_prefixes(*t, t == tree_procs.end() - 1);
			}

			//? Final sort based on tree index
			rng::stable_sort(current_procs, rng::less{}, &proc_info::tree_index);

			//? Move current selection/view to the selected process when collapsing/expanding in the tree
			if (locate_selection) {
				int loc = rng::find(current_procs, Proc::selected_pid, &proc_info::pid)->tree_index;
				if (Config::ints.at("proc_start") >= loc or Config::ints.at("proc_start") <= loc - Proc::select_max)
					Config::ints.at("proc_start") = max(0, loc - 1);
				Config::ints.at("proc_selected") = loc - Config::ints.at("proc_start") + 1;
			}
		}

		numpids = (int)current_procs.size() - filter_found;
		return current_procs;
	}
} // namespace Proc

namespace Tools {
	//? Uptime of the longest-running node
	double system_uptime() {
		double boot = 0;
		for (const auto& node : nodes)
			if (node.cur.boot > 0 and (boot == 0 or node.cur.boot < boot)) boot = node.cur.boot;
		return boot > 0 ? time(nullptr) - boot : 0.0;
	}
} // namespace Tools
