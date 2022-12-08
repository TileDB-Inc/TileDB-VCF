/**
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2021 TileDB, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

#include <CLI11.hpp>
#include <tiledb/tiledb>

#include "dataset/tiledbvcfdataset.h"
#include "read/export_format.h"
#include "read/reader.h"
#include "utils/logger_public.h"
#include "utils/utils.h"
#include "vcf/region.h"
#include "write/writer.h"

using namespace tiledb::vcf;

//==================================================================
// Command functions (do_*)
//==================================================================

void config_to_log(const CLI::App& cmd) {
  LOG_INFO("Version:\n{}", utils::version_info());
  LOG_INFO("Command options:\n{}", cmd.config_to_str(true));
}

/** Create. */
void do_create(const CreationParams& args, const CLI::App& cmd) {
  LOG_TRACE("Starting create command.");
  config_to_log(cmd);
  TileDBVCFDataset::create(args);
  LOG_TRACE("Finished create command.");
}

/** Register. */
void do_register(const RegistrationParams& args, const CLI::App& cmd) {
  LOG_TRACE("Starting register command.");
  config_to_log(cmd);

  // Set htslib global config and context based on user passed TileDB config
  // options
  utils::set_htslib_tiledb_context(args.tiledb_config);
  tiledb::Config cfg;
  utils::set_tiledb_config(args.tiledb_config, &cfg);
  TileDBVCFDataset dataset(cfg);
  dataset.open(args.uri, args.tiledb_config);
  if (dataset.metadata().version == TileDBVCFDataset::Version::V2 ||
      dataset.metadata().version == TileDBVCFDataset::Version::V3)
    dataset.register_samples(args);
  else {
    assert(dataset.metadata().version == TileDBVCFDataset::Version::V4);
    LOG_FATAL(
        "Only v2 and v3 datasets require registration. V4 and newer are "
        "capable of ingestion without registration.");
  }
  LOG_TRACE("Finished register command.");
}

/** Store/ingest. */
void do_store(const IngestionParams& args, const CLI::App& cmd) {
  if (args.sample_uris.size() == 0 && args.samples_file_uri.empty()) {
    std::cerr
        << "ERROR: RequiredError: VCF URIs or --sample-file is required\n";
    throw CLI::CallForHelp();
  }

  if (args.verbose) {
    LOG_SET_LEVEL("debug");
  }

  LOG_TRACE("Starting store command.");
  config_to_log(cmd);

  Writer writer;
  writer.set_all_params(args);
  writer.ingest_samples();

  if (args.tiledb_stats_enabled) {
    char* stats;
    writer.tiledb_stats(&stats);
    std::cout << "TileDB Internal Statistics:" << std::endl;
    std::cout << stats << std::endl;
  }
  LOG_TRACE("Finished store command.");
}

/** Export. */
void do_export(ExportParams& args, const CLI::App& cmd) {
  if (args.verbose) {
    LOG_SET_LEVEL("debug");
  }

  LOG_TRACE("Starting export command.");
  config_to_log(cmd);

  args.export_to_disk = !args.cli_count_only;

  Reader reader;
  reader.set_all_params(args);
  reader.open_dataset(args.uri);
  reader.read();

  if (args.tiledb_stats_enabled) {
    char* stats;
    reader.tiledb_stats(&stats);
    std::cout << "TileDB Internal Statistics:" << std::endl;
    std::cout << stats << std::endl;
  }
  LOG_TRACE("Finished export command.");
}

/** List. */
void do_list(const ListParams& args, const CLI::App& cmd) {
  LOG_TRACE("Starting list command.");
  config_to_log(cmd);

  // Set htslib global config and context based on user passed TileDB config
  // options
  utils::set_htslib_tiledb_context(args.tiledb_config);
  tiledb::Config cfg;
  utils::set_tiledb_config(args.tiledb_config, &cfg);
  TileDBVCFDataset dataset(cfg);
  dataset.open(args.uri, args.tiledb_config);
  dataset.print_samples_list();
  LOG_TRACE("Finished list command.");
}

/** Stat. */
void do_stat(const StatParams& args, const CLI::App& cmd) {
  LOG_TRACE("Starting stat command.");
  config_to_log(cmd);

  // Set htslib global config and context based on user passed TileDB config
  // options
  utils::set_htslib_tiledb_context(args.tiledb_config);
  tiledb::Config cfg;
  utils::set_tiledb_config(args.tiledb_config, &cfg);
  TileDBVCFDataset dataset(cfg);
  dataset.open(args.uri, args.tiledb_config);
  dataset.print_dataset_stats();
  LOG_TRACE("Finished stat command.");
}

/** Utils. */
void do_utils_consolidate_commits(
    const UtilsParams& args, const CLI::App& cmd) {
  LOG_TRACE("Starting utils consolidate commits command.");
  config_to_log(cmd);
  utils::set_htslib_tiledb_context(args.tiledb_config);
  tiledb::Config cfg;
  utils::set_tiledb_config(args.tiledb_config, &cfg);
  TileDBVCFDataset dataset(cfg);
  LOG_DEBUG("Consoldate commits.");
  dataset.consolidate_commits(args);
  LOG_TRACE("Finished utils consolidate commits command.");
}

void do_utils_consolidate_fragments(
    const UtilsParams& args, const CLI::App& cmd) {
  LOG_TRACE("Starting utils consolidate fragments command.");
  config_to_log(cmd);
  utils::set_htslib_tiledb_context(args.tiledb_config);
  tiledb::Config cfg;
  utils::set_tiledb_config(args.tiledb_config, &cfg);
  TileDBVCFDataset dataset(cfg);
  LOG_DEBUG("Consoldate fragments.");
  dataset.consolidate_fragments(args);
  LOG_TRACE("Finished utils consolidate fragments command.");
}

void do_utils_consolidate_fragment_metadata(
    const UtilsParams& args, const CLI::App& cmd) {
  LOG_TRACE("Starting utils consolidate fragment metadata command.");
  config_to_log(cmd);
  utils::set_htslib_tiledb_context(args.tiledb_config);
  tiledb::Config cfg;
  utils::set_tiledb_config(args.tiledb_config, &cfg);
  TileDBVCFDataset dataset(cfg);
  LOG_DEBUG("Consoldate fragment metadata.");
  dataset.consolidate_fragment_metadata(args);
  LOG_TRACE("Finished utils consolidate fragment metadata command.");
}

void do_utils_vacuum_commits(const UtilsParams& args, const CLI::App& cmd) {
  LOG_TRACE("Starting utils vacuum commits command.");
  config_to_log(cmd);
  utils::set_htslib_tiledb_context(args.tiledb_config);
  tiledb::Config cfg;
  utils::set_tiledb_config(args.tiledb_config, &cfg);
  TileDBVCFDataset dataset(cfg);
  LOG_DEBUG("Vacuum commits.");
  dataset.vacuum_commits(args);
  LOG_TRACE("Finished utils vacuum commits command.");
}

void do_utils_vacuum_fragments(const UtilsParams& args, const CLI::App& cmd) {
  LOG_TRACE("Starting utils vacuum fragments command.");
  config_to_log(cmd);
  utils::set_htslib_tiledb_context(args.tiledb_config);
  tiledb::Config cfg;
  utils::set_tiledb_config(args.tiledb_config, &cfg);
  TileDBVCFDataset dataset(cfg);
  LOG_DEBUG("Vacuum fragments.");
  dataset.vacuum_fragments(args);
  LOG_TRACE("Finished utils vacuum fragments command.");
}

void do_utils_vacuum_fragment_metadata(
    const UtilsParams& args, const CLI::App& cmd) {
  LOG_TRACE("Starting utils vacuum fragment metadata command.");
  LOG_DEBUG(cmd.config_to_str(true));
  utils::set_htslib_tiledb_context(args.tiledb_config);
  tiledb::Config cfg;
  utils::set_tiledb_config(args.tiledb_config, &cfg);
  TileDBVCFDataset dataset(cfg);
  LOG_DEBUG("Vacuum fragment metadata.");
  dataset.vacuum_fragment_metadata(args);
  LOG_TRACE("Finished utils vacuum fragment metadata command.");
}

//==================================================================
// cli parser helpers
//==================================================================

// maps for enums
std::map<std::string, tiledb_filter_type_t> filter_map{
    {"sha256", TILEDB_FILTER_CHECKSUM_SHA256},
    {"md5", TILEDB_FILTER_CHECKSUM_MD5},
    {"none", TILEDB_FILTER_NONE}};

std::map<std::string, ExportFormat> format_map{
    {"b", ExportFormat::CompressedBCF},
    {"u", ExportFormat::BCF},
    {"z", ExportFormat::VCFGZ},
    {"v", ExportFormat::VCF},
    {"t", ExportFormat::TSV}};

std::map<std::string, IngestionParams::ContigMode> contig_mode_map{
    {"all", IngestionParams::ContigMode::ALL},
    {"separate", IngestionParams::ContigMode::SEPARATE},
    {"merged", IngestionParams::ContigMode::MERGED}};

// add helper functions to CLI::detail namespace
namespace CLI {
namespace detail {

// lexical_cast is used to convert string to a custom type
template <>
bool lexical_cast<PartitionInfo>(
    const std::string& input, PartitionInfo& output) {
  auto vals = CLI::detail::split(input, ':');
  if (vals.size() != 2) {
    return false;
  }

  try {
    output.partition_index = std::stoul(vals.at(0));
    output.num_partitions = std::stoul(vals.at(1));
  } catch (const std::exception& e) {
    return false;
  }

  return true;
}

// overload << to display key from enum map in help messages
std::ostream& operator<<(std::ostream& os, const tiledb_filter_type_t& value) {
  for (auto it : filter_map) {
    if (it.second == value) {
      os << it.first;
      return os;
    }
  }
  os << "UNKNOWN";
  return os;
}

std::ostream& operator<<(std::ostream& os, const ExportFormat& value) {
  for (auto it : format_map) {
    if (it.second == value) {
      os << it.first;
      return os;
    }
  }
  os << "UNKNOWN";
  return os;
}

std::ostream& operator<<(
    std::ostream& os, const IngestionParams::ContigMode& value) {
  for (auto it : contig_mode_map) {
    if (it.second == value) {
      os << it.first;
      return os;
    }
  }
  os << "UNKNOWN";
  return os;
}

}  // namespace detail
}  // namespace CLI

// return string with newlines inserted near the specified width
std::string wrap(const std::string& input, const int width = 80) {
  std::stringstream ss;
  int col = 0;
  for (auto chr : input) {
    if (col >= width && chr == ' ') {
      ss << std::endl;
      col = 0;
    } else {
      ss << chr;
      col++;
    }
  }
  return ss.str();
}

// custom cli11 formater that line wraps option descriptions
class VcfFormatter : public CLI::Formatter {
 public:
  VcfFormatter(int column_width = 80)
      : Formatter()
      , column_width_(column_width) {
  }

  std::string make_option_desc(const CLI::Option* opt) const override {
    return wrap(opt->get_description(), column_width_);
  }

 private:
  int column_width_;
};

//==================================================================
// cli parser common options
//==================================================================

void add_tiledb_uri_option(CLI::App* cmd, std::string& uri) {
  cmd->add_option("-u,--uri", uri, "TileDB-VCF dataset URI")->required();
}

void add_tiledb_options(
    CLI::App* cmd, std::vector<std::string>& tiledb_config) {
  cmd->add_option(
         "--tiledb-config",
         tiledb_config,
         "CSV string of the format 'param1=val1,param2=val2...' "
         "specifying optional TileDB configuration parameter settings.")
      ->delimiter(',');
}

void add_logging_options(
    CLI::App* cmd, std::string& log_level, std::string& log_file) {
  cmd->add_option_function<std::string>(
         "--log-level",
         [](const std::string& value) { LOG_SET_LEVEL(value); },
         "Log message level")
      ->default_str("fatal")
      ->check(CLI::IsMember(
          {"fatal", "error", "warn", "info", "debug", "trace"},
          CLI::ignore_case));
  cmd->add_option_function<std::string>(
      "--log-file",
      [](const std::string& value) { LOG_SET_FILE(value); },
      "Log message output file");
}

//==================================================================
// cli parser subcommands (add_*)
//==================================================================

void add_create(CLI::App& app) {
  auto args = std::make_shared<CreationParams>();
  auto cmd =
      app.add_subcommand("create", "Creates an empty TileDB-VCF dataset");

  cmd->set_help_flag("-h,--help")->group("");  // hide from help message
  add_tiledb_uri_option(cmd, args->uri);
  cmd->add_option(
         "-a,--attributes",
         args->extra_attributes,
         "INFO and/or FORMAT field names (comma-delimited) to store as "
         "separate attributes. Names should be 'fmt_X' or 'info_X' for "
         "a field name 'X' (case sensitive).")
      ->delimiter(',');
  cmd->add_option(
         "-v,--vcf-attributes",
         args->vcf_uri,
         "Create separate attributes for all INFO and FORMAT fields in the "
         "provided VCF file.")
      ->excludes("--attributes");
  cmd->add_option(
      "-g,--anchor-gap", args->anchor_gap, "Anchor gap size to use");
  cmd->add_flag_function(
      "-n,--no-duplicates",
      [args](int count) { args->allow_duplicates = false; },
      "Allow records with duplicate start positions to be written to the "
      "array.");
  cmd->add_flag(
      "--compress-sample-dim",
      args->compress_sample_dim,
      "Add zstd compression to the sample dimension.");

  cmd->option_defaults()->group("Ingestion task options");
  cmd->add_flag(
      "--enable-allele-count",
      args->enable_allele_count,
      "Enable allele count ingestion task");
  cmd->add_flag(
      "--enable-variant-stats",
      args->enable_variant_stats,
      "Enable variant stats ingestion task");

  cmd->option_defaults()->group("TileDB options");
  cmd->add_option(
      "-c,--tile-capacity",
      args->tile_capacity,
      "Tile capacity to use for the array schema");
  add_tiledb_options(cmd, args->tiledb_config);
  cmd->add_option(
         "--checksum",
         args->checksum,
         "Checksum to use for dataset validation on read and writes.")
      ->transform(CLI::CheckedTransformer(filter_map));

  cmd->option_defaults()->group("Debug options");
  add_logging_options(cmd, args->log_level, args->log_file);

  // register function to implement this command
  cmd->callback([args, cmd]() { do_create(*args, *cmd); });
}

void add_register(CLI::App& app) {
  auto args = std::make_shared<RegistrationParams>();
  auto cmd = app.add_subcommand(
      "register",
      "Registers samples in a TileDB-VCF dataset prior to ingestion");

  cmd->group("");  // hide register command from help message
  cmd->set_help_flag("-h,--help")->group("");  // hide from help message
  add_tiledb_uri_option(cmd, args->uri);
  cmd->add_option(
      "-d,--scratch-dir",
      args->scratch_space.path,
      "Directory used for local storage of downloaded remote samples");
  cmd->add_option(
      "-s,--scratch-mb",
      args->scratch_space.size_mb,
      "Amount of local storage that can be used for downloading remote samples "
      "(MB)");
  add_tiledb_options(cmd, args->tiledb_config);
  cmd->add_option(
      "-f,--samples-file",
      args->sample_uris_file,
      "File with 1 VCF path to be ingested per line. The format can "
      "also include an explicit index path on each line, in the format "
      "'<vcf-uri><TAB><index-uri>'");
  cmd->add_option("paths", args->sample_uris, "VCF URIs to ingest")
      ->excludes("--samples-file");

  cmd->option_defaults()->group("Debug options");
  add_logging_options(cmd, args->log_level, args->log_file);

  // register function to implement this command
  cmd->callback([args, cmd]() { do_register(*args, *cmd); });
}

void add_store(CLI::App& app) {
  auto args = std::make_shared<IngestionParams>();
  auto cmd =
      app.add_subcommand("store", "Ingests samples into a TileDB-VCF dataset");

  cmd->set_help_flag("-h,--help")->group("");  // hide from help message
  add_tiledb_uri_option(cmd, args->uri);
  cmd->add_option("-t,--threads", args->num_threads, "Number of threads");
  cmd->add_option(
         "-m,--total-memory-budget-mb",
         args->total_memory_budget_mb,
         "The total memory budget for ingestion (MiB)")
      ->check(CLI::Range(512u, utils::system_memory_mb()));
  cmd->add_option(
         "-M,--total-memory-percentage",
         args->total_memory_percentage,
         "Percentage of total system memory used for ingestion "
         "(overrides '--total-memory-budget-mb')")
      ->check(CLI::Range(0.0, 1.0));
  cmd->add_flag(
      "--resume",
      args->resume_sample_partial_ingestion,
      "Resume incomplete ingestion of sample batch");

  cmd->option_defaults()->group("Sample options");
  cmd->add_option(
      "-e,--sample-batch-size",
      args->sample_batch_size,
      "Number of samples per batch for ingestion");
  cmd->add_option(
      "-f,--samples-file",
      args->samples_file_uri,
      "File with 1 VCF path to be ingested per line. The format can "
      "also include an explicit index path on each line, in the format "
      "'<vcf-uri><TAB><index-uri>'");
  cmd->add_option("paths", args->sample_uris, "VCF URIs to ingest")
      ->excludes("--samples-file");
  cmd->add_flag(
         "--remove-sample-file",
         args->remove_samples_file,
         "If specified, the samples file ('-f' argument) is deleted after "
         "successful ingestion")
      ->needs("--samples-file");
  cmd->add_option(
      "-d,--scratch-dir",
      args->scratch_space.path,
      "Directory used for local storage of downloaded remote samples");
  cmd->add_option(
      "-s,--scratch-mb",
      args->scratch_space.size_mb,
      "Amount of local storage that can be used for downloading remote samples "
      "(MB)");

  cmd->option_defaults()->group("TileDB options");
  cmd->add_option(
      "-p,--s3-part-size",
      args->part_size_mb,
      "[S3 only] Part size to use for writes (MB)");
  add_tiledb_options(cmd, args->tiledb_config);
  cmd->add_flag("--stats", args->tiledb_stats_enabled, "Enable TileDB stats");
  cmd->add_flag(
      "--stats-vcf-header-array",
      args->tiledb_stats_enabled_vcf_header_array,
      "Enable TileDB stats for vcf header array usage");

  cmd->option_defaults()->group("Advanced options");
  cmd->add_option(
         "--ratio-tiledb-memory",
         args->ratio_tiledb_memory,
         "Ratio of memory budget allocated to TileDB::sm.mem.total_budget")
      ->check(CLI::Range(0.01, 0.99));
  cmd->add_option(
      "--max-tiledb-memory-mb",
      args->max_tiledb_memory_mb,
      "Maximum memory allocated to TileDB::sm.mem.total_budget (MiB)");
  cmd->add_option(
      "--input-record-buffer-mb",
      args->input_record_buffer_mb,
      "Size of input record buffer for each sample file (MiB)");
  cmd->add_option(
         "--avg-vcf-record-size",
         args->avg_vcf_record_size,
         "Average VCF record size (bytes)")
      ->check(CLI::Range(1, 4096));
  cmd->add_option(
         "--ratio-task-size",
         args->ratio_task_size,
         "Ratio of worker task size to computed task size")
      ->check(CLI::Range(0.01, 1.0));
  cmd->add_option(
         "--ratio-output-flush",
         args->ratio_output_flush,
         "Ratio of output buffer capacity that triggers a flush to TileDB")
      ->check(CLI::Range(0.01, 1.0));

  cmd->option_defaults()->group("Contig options");
  cmd->add_flag(
      "--disable-contig-fragment-merging",
      args->contig_fragment_merging,
      "Disable merging of contigs into fragments. Generally contig fragment "
      "merging is good, this is a performance optimization to reduce the "
      "prefixes on a s3/azure/gcs bucket when there is a large number of "
      "pseudo contigs which are small in size.");
  cmd->add_option(
         "--contigs-to-keep-separate",
         args->contigs_to_keep_separate,
         "Comma-separated list of contigs that should not be merged "
         "into combined fragments. The default list includes all "
         "standard human chromosomes in both UCSC (e.g., chr1) and "
         "Ensembl (e.g., 1) formats.")
      ->delimiter(',')
      ->default_str("")
      ->excludes("--disable-contig-fragment-merging");
  cmd->add_option(
         "--contigs-to-allow-merging",
         args->contigs_to_allow_merging,
         "Comma-separated list of contigs that should be allowed to "
         "be merged into combined fragments.")
      ->delimiter(',')
      ->excludes("--disable-contig-fragment-merging")
      ->excludes("--contigs-to-keep-separate");
  cmd->add_option(
         "--contig-mode",
         args->contig_mode,
         "Select which contigs are ingested: 'separate', 'merged', or 'all' "
         "contigs")
      ->transform(CLI::CheckedTransformer(contig_mode_map));

  cmd->option_defaults()->group("Debug options");
  add_logging_options(cmd, args->log_level, args->log_file);
  cmd->add_flag("-v,--verbose", args->verbose, "Enable verbose output");
  CLI::deprecate_option(cmd, "--verbose", "--log-level debug");

  cmd->option_defaults()->group("Legacy options");
  cmd->add_option_function<unsigned>(
      "-n,--max-record-buff",
      [args](const unsigned& value) {
        args->max_record_buffer_size = value;
        args->use_legacy_max_record_buffer_size = true;
      },
      "Max number of VCF records to buffer per file");
  cmd->add_option_function<unsigned>(
      "-k,--thread-task-size",
      [args](const unsigned& value) {
        args->thread_task_size = value;
        args->use_legacy_thread_task_size = true;
      },
      "Max length (# columns) of an ingestion task. Affects load "
      "balancing of ingestion work across threads, and total "
      "memory consumption.");
  cmd->add_option_function<unsigned>(
      "-b,--mem-budget-mb",
      [args](const unsigned& value) {
        args->max_tiledb_buffer_size_mb = value;
        args->use_legacy_max_tiledb_buffer_size_mb = true;
      },
      "The maximum size of TileDB buffers before flushing (MiB)");

  // register function to implement this command
  cmd->callback([args, cmd]() { do_store(*args, *cmd); });
}

void add_export(CLI::App& app) {
  auto args = std::make_shared<ExportParams>();
  auto cmd =
      app.add_subcommand("export", "Exports data from a TileDB-VCF dataset");

  cmd->set_help_flag("-h,--help")->group("");  // hide from help message
  add_tiledb_uri_option(cmd, args->uri);

  cmd->option_defaults()->group("Output options");
  cmd->add_option(
         "-O,--output-format",
         args->format,
         "Export format. Options are: 'b': bcf (compressed); 'u': bcf; "
         "'z': vcf.gz; 'v': vcf; 't': TSV")
      ->transform(CLI::CheckedTransformer(format_map));
  cmd->add_option(
      "-o,--output-path",
      args->output_path,
      "[TSV or combined VCF export only] The name of the output file.");
  cmd->add_flag(
         "-m,--merge", args->export_combined_vcf, "Export combined VCF file.")
      ->needs("--output-path");
  cmd->add_option(
         "-t,--tsv-fields",
         args->tsv_fields,
         "[TSV export only] An ordered CSV list of fields to export in "
         "the TSV. A field name can be one of 'SAMPLE', 'ID', 'REF', "
         "'ALT', 'QUAL', 'POS', 'CHR', 'FILTER'. Additionally, INFO "
         "fields can be specified by 'I:<name>' and FMT fields with "
         "'F:<name>'. To export the intersecting query region for each "
         "row in the output, use the field names 'Q:POS', 'Q:END' and "
         "'Q:LINE'.")
      ->delimiter(',');
  cmd->add_option(
      "-n,--limit",
      args->max_num_records,
      "Only export the first N intersecting records.");
  cmd->add_option(
      "-d,--output-dir",
      args->output_dir,
      "Directory used for local output of exported samples");
  cmd->add_option(
      "--upload-dir",
      args->upload_dir,
      "If set, all output file(s) from the export process will be "
      "copied to the given directory (or S3 prefix) upon completion.");
  cmd->add_flag(
      "-c,--count-only",
      args->cli_count_only,
      "Don't write output files, only print the count of the resulting "
      "number of intersecting records.");
  cmd->add_option(
         "--af-filter",
         args->af_filter,
         "If set, only export data that passes the AF filter.")
      ->excludes("--count-only");

  cmd->option_defaults()->group("Region options");
  cmd->add_option(
         "-r,--regions",
         args->regions,
         "CSV list of regions to export in the format 'chr:min-max'")
      ->delimiter(',');
  cmd->add_option(
         "-R,--regions-file",
         args->regions_file_uri,
         "File containing regions (BED format)")
      ->excludes("--regions");
  cmd->add_flag(
      "--sorted",
      args->sort_regions,
      "Do not sort regions or regions file if they are pre-sorted");
  cmd->add_option(
      "--region-partition",
      args->region_partitioning,
      "Partitions the list of regions to be exported and causes this "
      "export to export only a specific partition of them. Specify in "
      "the format I:N where I is the partition index and N is the "
      "total number of partitions. Useful for batch exports.");

  cmd->option_defaults()->group("Sample options");
  cmd->add_option(
      "-f,--samples-file",
      args->samples_file_uri,
      "Path to file with 1 sample name per line");
  cmd->add_option(
         "-s,--sample-names",
         args->sample_names,
         "CSV list of sample names to export")
      ->delimiter(',')
      ->excludes("--samples-file");
  cmd->add_option(
      "--sample-partition",
      args->sample_partitioning,
      "Partitions the list of samples to be exported and causes this "
      "export to export only a specific partition of them. Specify in "
      "the format I:N where I is the partition index and N is the "
      "total number of partitions. Useful for batch exports.");
  cmd->add_flag(
      "--disable-check-samples",
      args->check_samples_exist,
      "Disable validating that sample passed exist in dataset before "
      "executing query and error if any sample requested is not in the "
      "dataset");

  cmd->option_defaults()->group("TileDB options");
  add_tiledb_options(cmd, args->tiledb_config);
  cmd->add_option(
      "--mem-budget-buffer-percentage",
      args->memory_budget_breakdown.buffers_percentage,
      "The percentage of the memory budget to use for TileDB "
      "query buffers.");
  cmd->add_option(
      "--mem-budget-tile-cache-percentage",
      args->memory_budget_breakdown.tile_cache_percentage,
      "The percentage of the memory budget to use for TileDB tile "
      "cache.");
  cmd->add_option(
      "-b,--mem-budget-mb",
      args->memory_budget_mb,
      "The memory budget (MB) used when submitting TileDB "
      "queries.");

  cmd->add_flag("--stats", args->tiledb_stats_enabled, "Enable TileDB stats");
  cmd->add_flag(
      "--stats-vcf-header-array",
      args->tiledb_stats_enabled_vcf_header_array,
      "Enable TileDB stats for vcf header array usage");

  cmd->option_defaults()->group("Debug options");
  add_logging_options(cmd, args->log_level, args->log_file);
  cmd->add_flag("-v,--verbose", args->verbose, "Enable verbose output");
  CLI::deprecate_option(cmd, "--verbose", "--log-level debug");
  cmd->add_flag(
      "--enable-progress-estimation",
      args->enable_progress_estimation,
      "Enable progress estimation in verbose mode. Progress estimation "
      "can sometimes cause a performance impact, so enable this with "
      "consideration.");
  cmd->add_flag(
      "--debug-print-vcf-regions",
      args->debug_params.print_vcf_regions,
      "Enable debug printing of vcf region passed by user or bed file. "
      "Requires verbose mode");
  cmd->add_flag(
      "--debug-print-sample-list",
      args->debug_params.print_sample_list,
      "Enable debug printing of sample list used in read. Requires "
      "verbose mode");
  cmd->add_flag(
      "--debug-print-tiledb-query-ranges",
      args->debug_params.print_tiledb_query_ranges,
      "Enable debug printing of tiledb query ranges used in read. "
      "Requires verbose mode");

  // register function to implement this command
  cmd->callback([args, cmd]() { do_export(*args, *cmd); });
}

void add_list(CLI::App& app) {
  auto args = std::make_shared<ListParams>();
  auto cmd = app.add_subcommand(
      "list", "Lists all sample names present in a TileDB-VCF dataset");
  cmd->set_help_flag("-h,--help")->group("");  // hide from help message
  add_tiledb_uri_option(cmd, args->uri);
  add_tiledb_options(cmd, args->tiledb_config);
  add_logging_options(cmd, args->log_level, args->log_file);

  // register function to implement this command
  cmd->callback([args, cmd]() { do_list(*args, *cmd); });
}

void add_stat(CLI::App& app) {
  auto args = std::make_shared<StatParams>();
  auto cmd = app.add_subcommand(
      "stat", "Prints high-level statistics about a TileDB-VCF dataset");
  cmd->set_help_flag("-h,--help")->group("");  // hide from help message
  add_tiledb_uri_option(cmd, args->uri);
  add_tiledb_options(cmd, args->tiledb_config);
  add_logging_options(cmd, args->log_level, args->log_file);

  // register function to implement this command
  cmd->callback([args, cmd]() { do_stat(*args, *cmd); });
}

void add_util_options(CLI::App* cmd, UtilsParams& args) {
  cmd->set_help_flag("-h,--help")->group("");  // hide from help message
  add_tiledb_uri_option(cmd, args.uri);
  add_tiledb_options(cmd, args.tiledb_config);
  add_logging_options(cmd, args.log_level, args.log_file);
}

void add_utils(CLI::App& app) {
  auto args = std::make_shared<UtilsParams>();
  auto cmd = app.add_subcommand(
      "utils", "Utils for working with a TileDB-VCF dataset");
  cmd->require_subcommand(1, 1);

  auto c_cmd =
      cmd->add_subcommand("consolidate", "Consolidate TileDB-VCF dataset");
  c_cmd->require_subcommand(1, 1);

  auto c_c_cmd = c_cmd->add_subcommand(
      "commits", "Consolidate TileDB-VCF dataset commits");
  add_util_options(c_c_cmd, *args);
  c_c_cmd->callback(
      [args, cmd]() { do_utils_consolidate_commits(*args, *cmd); });

  auto c_f_cmd = c_cmd->add_subcommand(
      "fragments", "Consolidate TileDB-VCF dataset fragments");
  add_util_options(c_f_cmd, *args);
  c_f_cmd->callback(
      [args, cmd]() { do_utils_consolidate_fragments(*args, *cmd); });

  auto c_m_cmd = c_cmd->add_subcommand(
      "fragment_meta", "Consolidate TileDB-VCF dataset fragment metadata");
  add_util_options(c_m_cmd, *args);
  c_m_cmd->callback(
      [args, cmd]() { do_utils_consolidate_fragment_metadata(*args, *cmd); });

  auto v_cmd = cmd->add_subcommand("vacuum", "Vacuum TileDB-VCF dataset");
  v_cmd->require_subcommand(1, 1);

  auto v_c_cmd =
      v_cmd->add_subcommand("commits", "Vacuum TileDB-VCF dataset commits");
  add_util_options(v_c_cmd, *args);
  v_c_cmd->callback([args, cmd]() { do_utils_vacuum_commits(*args, *cmd); });

  auto v_f_cmd =
      v_cmd->add_subcommand("fragments", "Vacuum TileDB-VCF dataset fragments");
  add_util_options(v_f_cmd, *args);
  v_f_cmd->callback([args, cmd]() { do_utils_vacuum_fragments(*args, *cmd); });

  auto v_m_cmd = v_cmd->add_subcommand(
      "fragment_meta", "Vacuum TileDB-VCF dataset fragment metadata");
  add_util_options(v_m_cmd, *args);
  v_m_cmd->callback(
      [args, cmd]() { do_utils_vacuum_fragment_metadata(*args, *cmd); });
}

//==================================================================
// main program
//==================================================================

int main(int argc, char** argv) {
  // column widths for help message
  int left_width = 40;
  int right_width = 80;

  CLI::App app{
      "TileDB-VCF -- Efficient variant-call data storage and retrieval.\n\n"
      "  This command-line utility provides an interface to create, store and\n"
      "  efficiently retrieve variant-call data in the TileDB storage "
      "format.\n\n"
      "  More information: TileDB <https://tiledb.com>"};
  app.formatter(std::make_shared<VcfFormatter>(right_width));
  app.get_formatter()->column_width(left_width);
  app.failure_message(CLI::FailureMessage::help);
  app.require_subcommand(1, 1);
  app.option_defaults()->always_capture_default();

  // add subcommands
  add_create(app);
  add_register(app);
  add_store(app);
  add_export(app);
  add_list(app);
  add_stat(app);
  add_utils(app);

  // add version option and subcommand
  app.add_flag_function(
      "-v,--version",
      [](int count) {
        std::cout << utils::version_info() << std::endl;
        exit(0);
      },
      "Print the version information and exit");
  auto sub =
      app.add_subcommand("version", "Print the version information and exit");
  sub->parse_complete_callback([]() {
    std::cout << utils::version_info() << std::endl;
    exit(0);
  });

  try {
    CLI11_PARSE(app, argc, argv);
  } catch (const std::exception& e) {
    LOG_FATAL("Exception: {}", e.what());
  }

  return 0;
}
