/**
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2018 TileDB, Inc.
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

#include <clipp.h>
#include <sstream>
#include <thread>

#include "dataset/tiledbvcfdataset.h"
#include "read/export_format.h"
#include "read/reader.h"
#include "utils/utils.h"
#include "vcf/region.h"
#include "vcf/vcf.h"
#include "write/writer.h"

using namespace tiledb::vcf;

namespace {
/** TileDBVCF operation mode */
enum class Mode { Version, Create, Register, Store, Export, List, Stat, UNDEF };

/** Returns a help string, displaying the given default value. */
template <typename T>
std::string defaulthelp(const std::string& msg, T default_value) {
  return msg + " [default " + std::to_string(default_value) + "]";
}

/** Returns TileDB-VCF and TileDB version information in string form. */
std::string version_info() {
  std::stringstream ss;
  ss << "TileDB-VCF build " << utils::TILEDB_VCF_COMMIT_HASH << "\n";
  auto v = tiledb::version();
  ss << "TileDB version " << std::get<0>(v) << "." << std::get<1>(v) << "."
     << std::get<2>(v);
  return ss.str();
}

/** Prints a formatted help message for a command. */
void print_command_usage(
    const std::string& name, const std::string& desc, const clipp::group& cli) {
  using namespace clipp;
  clipp::doc_formatting fmt{};
  fmt.start_column(4).doc_column(25);
  std::cout << name << "\n\nDESCRIPTION\n    " << desc << "\n\nUSAGE\n"
            << usage_lines(cli, name, fmt) << "\n\nOPTIONS\n"
            << documentation(cli, fmt) << "\n";
}

/** Prints the 'create' mode help message. */
void usage_create(const clipp::group& create_mode) {
  print_command_usage(
      "tiledbvcf create", "Creates an empty TileDB-VCF dataset.", create_mode);
}

/** Prints the 'register' mode help message. */
void usage_register(const clipp::group& register_mode) {
  print_command_usage(
      "tiledbvcf register",
      "Registers samples in a TileDB-VCF dataset prior to ingestion.",
      register_mode);
}

/** Prints the 'store' mode help message. */
void usage_store(const clipp::group& store_mode) {
  print_command_usage(
      "tiledbvcf store",
      "Ingests registered samples into a TileDB-VCF dataset.",
      store_mode);
}

/** Prints the 'export' mode help message. */
void usage_export(const clipp::group& export_mode) {
  print_command_usage(
      "tiledbvcf export",
      "Exports data from a TileDB-VCF dataset.",
      export_mode);
}

/** Prints the 'list' mode help message. */
void usage_list(const clipp::group& list_mode) {
  print_command_usage(
      "tiledbvcf list",
      "Lists all sample names present in a TileDB-VCF dataset.",
      list_mode);
}

/** Prints the 'stat' mode help message. */
void usage_stat(const clipp::group& stat_mode) {
  print_command_usage(
      "tiledbvcf stat",
      "Prints high-level statistics about a TileDB-VCF dataset.",
      stat_mode);
}

/** Prints the default help message. */
void usage(
    const clipp::group& cli,
    const clipp::group& create_mode,
    const clipp::group& register_mode,
    const clipp::group& store_mode,
    const clipp::group& export_mode,
    const clipp::group& list_mode,
    const clipp::group& stat_mode) {
  using namespace clipp;
  std::cout
      << "TileDBVCF -- efficient variant-call data storage and retrieval.\n\n"
      << "This command-line utility provides an interface to create, store and "
         "efficiently retrieve variant-call data in the TileDB storage format."
      << "\n\n"
      << "More information: TileDB <https://tiledb.io>\n"
      << version_info() << "\n\n";

  std::cout << "Summary:\n" << usage_lines(cli, "tiledbvcf") << "\n\n\n";
  usage_create(create_mode);
  std::cout << "\n\n";
  usage_register(register_mode);
  std::cout << "\n\n";
  usage_store(store_mode);
  std::cout << "\n\n";
  usage_export(export_mode);
  std::cout << "\n\n";
  usage_list(list_mode);
  std::cout << "\n\n";
  usage_stat(stat_mode);
  std::cout << "\n";
}

/** Parses the string into the given partition info struct. */
void set_partitioning(const std::string& s, PartitionInfo* info) {
  auto vals = utils::split(s, ':');
  try {
    info->partition_index = (unsigned)std::stoul(vals.at(0));
    info->num_partitions = (unsigned)std::stoul(vals.at(1));
  } catch (const std::exception& e) {
    throw std::invalid_argument(
        "Error parsing partition string '" + s + "': " + std::string(e.what()));
  }
}

/** Create. */
void do_create(const CreationParams& args) {
  TileDBVCFDataset::create(args);
}

/** Register. */
void do_register(const RegistrationParams& args) {
  TileDBVCFDataset dataset;
  dataset.open(args.uri, args.tiledb_config);
  dataset.register_samples(args);
}

/** Store/ingest. */
void do_store(const IngestionParams& args) {
  Writer writer;
  writer.set_all_params(args);
  writer.ingest_samples();
}

/** Export. */
void do_export(const ExportParams& args) {
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
}

/** List. */
void do_list(const ListParams& args) {
  TileDBVCFDataset dataset;
  dataset.open(args.uri, args.tiledb_config);
  dataset.print_samples_list();
}

/** Stat. */
void do_stat(const StatParams& args) {
  TileDBVCFDataset dataset;
  dataset.open(args.uri, args.tiledb_config);
  dataset.print_dataset_stats();
}

}  // namespace

int main(int argc, char** argv) {
  using namespace clipp;
  Mode opmode = Mode::UNDEF;

  CreationParams create_args;
  auto create_mode =
      (required("-u", "--uri") % "TileDB dataset URI" &
           value("uri", create_args.uri),
       option("-a", "--attributes") %
               "Info or format field names (comma-delimited) to store as "
               "separate attributes. Names should be 'fmt_X' or 'info_X' for "
               "a field name 'X' (case sensitive)." &
           value("fields").call([&](const std::string& s) {
             create_args.extra_attributes = utils::split(s, ',');
           }),
       option("-c", "--tile-capacity") %
               defaulthelp(
                   "Tile capacity to use for the array schema",
                   create_args.tile_capacity) &
           value("N", create_args.tile_capacity),
       option("-e", "--tile-extent") %
               defaulthelp(
                   "Row tile extent to use for the array schema",
                   create_args.row_tile_extent) &
           value("N", create_args.row_tile_extent),
       option("-g", "--anchor-gap") %
               defaulthelp("Anchor gap size to use", create_args.anchor_gap) &
           value("N", create_args.anchor_gap),
       option("--tiledb-config") %
               "CSV string of the format 'param1=val1,param2=val2...' "
               "specifying optional TileDB configuration parameter settings." &
           value("params").call([&create_args](const std::string& s) {
             create_args.tiledb_config = utils::split(s, ',');
           }),
       option("--checksum") %
               "Checksum to use for dataset validation on read and writes, "
               "defaults to 'sha256'" &
           value("checksum").call([&create_args](const std::string& s) {
             if (s == "sha256")
               create_args.checksum = TILEDB_FILTER_CHECKSUM_SHA256;
             else if (s == "md5")
               create_args.checksum = TILEDB_FILTER_CHECKSUM_MD5;
             else if (s == "none")
               create_args.checksum = TILEDB_FILTER_NONE;
           }),
       option("-n", "--no-duplicates")
               .set(create_args.allow_duplicates, false) %
           "Do not allow records with duplicate end positions to be written to "
           "the array.");

  RegistrationParams register_args;
  auto register_mode =
      (required("-u", "--uri") % "TileDB dataset URI" &
           value("uri", register_args.uri),
       option("-d", "--scratch-dir") %
               "Directory used for local storage of downloaded remote samples" &
           value("path", register_args.scratch_space.path),
       option("-s", "--scratch-mb") %
               "Amount of local storage that can be used for downloading "
               "remote samples (MB)" &
           value("MB", register_args.scratch_space.size_mb),
       option("--tiledb-config") %
               "CSV string of the format 'param1=val1,param2=val2...' "
               "specifying optional TileDB configuration parameter settings." &
           value("params").call([&register_args](const std::string& s) {
             register_args.tiledb_config = utils::split(s, ',');
           }),
       (option("-f", "--samples-file") %
            "File with 1 VCF path to be registered per line. The format can "
            "also include an explicit index path on each line, in the format "
            "'<vcf-uri><TAB><index-uri>'" &
        value("path", register_args.sample_uris_file)) |
           (values("paths", register_args.sample_uris) %
            "Argument list of VCF files to register"));

  IngestionParams store_args;
  auto store_mode =
      (required("-u", "--uri") % "TileDB dataset URI" &
           value("uri", store_args.uri),
       option("-t", "--threads") %
               defaulthelp("Number of threads", store_args.num_threads) &
           value("N", store_args.num_threads),
       option("-p", "--s3-part-size") %
               defaulthelp(
                   "[S3 only] Part size to use for writes (MB)",
                   store_args.part_size_mb) &
           value("MB", store_args.part_size_mb),
       option("-d", "--scratch-dir") %
               "Directory used for local storage of downloaded remote samples" &
           value("path", store_args.scratch_space.path),
       option("-s", "--scratch-mb") %
               defaulthelp(
                   "Amount of local storage that can be used for downloading "
                   "remote samples (MB)",
                   store_args.scratch_space.size_mb) &
           value("MB", store_args.scratch_space.size_mb),
       option("-n", "--max-record-buff") %
               defaulthelp(
                   "Max number of BCF records to buffer per file",
                   store_args.max_record_buffer_size) &
           value("N", store_args.max_record_buffer_size),
       option("-k", "--thread-task-size") %
               defaulthelp(
                   "Max length (# columns) of an ingestion task. Affects load "
                   "balancing of ingestion work across threads, and total "
                   "memory consumption.",
                   store_args.thread_task_size) &
           value("N", store_args.thread_task_size),
       option("-v", "--verbose").set(store_args.verbose) %
           "Enable verbose output",
       option("--remove-sample-file").set(store_args.remove_samples_file) %
           "If specified, the samples file ('-f' argument) is deleted after "
           "successful ingestion",
       option("--tiledb-config") %
               "CSV string of the format 'param1=val1,param2=val2...' "
               "specifying optional TileDB configuration parameter settings." &
           value("params").call([&store_args](const std::string& s) {
             store_args.tiledb_config = utils::split(s, ',');
           }),
       (option("-f", "--samples-file") %
            "File with 1 VCF path to be ingested per line. The format can "
            "also include an explicit index path on each line, in the format "
            "'<vcf-uri><TAB><index-uri>'" &
        value("path", store_args.samples_file_uri)) |
           (values("paths", store_args.sample_uris) %
            "Argument list of VCF files to ingest"));

  ExportParams export_args;
  export_args.export_to_disk = true;
  auto export_mode =
      (required("-u", "--uri") % "TileDB dataset URI" &
           value("uri", export_args.uri),
       option("-O", "--output-format") %
               "Export format. Options are: 'b': bcf (compressed); 'u': bcf; "
               "'z': vcf.gz; 'v': vcf; 't': TSV. [default b]" &
           value("format").call([&export_args](const std::string& s) {
             const std::map<std::string, ExportFormat> m = {
                 {"b", ExportFormat::CompressedBCF},
                 {"u", ExportFormat::BCF},
                 {"z", ExportFormat::VCFGZ},
                 {"v", ExportFormat::VCF},
                 {"t", ExportFormat::TSV},
             };
             auto it = m.find(s);
             if (it == m.end())
               throw std::invalid_argument("Unknown export format '" + s + "'");
             export_args.format = it->second;
           }),
       option("-o", "--output-path") %
               "[TSV export only] The name of the output TSV file." &
           value("path", export_args.tsv_output_path),
       option("-t", "--tsv-fields") %
               "[TSV export only] An ordered CSV list of fields to export in "
               "the TSV. A field name can be one of 'SAMPLE', 'ID', 'REF', "
               "'ALT', 'QUAL', 'POS', 'CHR', 'FILTER'. Additionally, INFO "
               "fields can be specified by 'I:<name>' and FMT fields with "
               "'S:<name>'. To export the intersecting query region for each "
               "row in the output, use the field names 'Q:POS' and 'Q:END'." &
           value("fields").call([&export_args](const std::string& s) {
             export_args.tsv_fields = utils::split(s, ',');
           }),
       ((option("-r", "--regions") %
             "CSV list of regions to export in the format 'chr:min-max'" &
         value("regions").call([&export_args](const std::string& s) {
           export_args.regions = utils::split(s, ',');
         })) |
        (option("-R", "--regions-file") %
             "File containing regions (BED format)" &
         value("path", export_args.regions_file_uri))),
       option("-n", "--limit") %
               "Only export the first N intersecting records." &
           value("N", export_args.max_num_records),
       option("-d", "--output-dir") %
               "Directory used for local output of exported samples" &
           value("path", export_args.output_dir),
       option("--sample-partition") %
               "Partitions the list of samples to be exported and causes this "
               "export to export only a specific partition of them. Specify in "
               "the format I:N where I is the partition index and N is the "
               "total number of partitions. Useful for batch exports." &
           value("I:N").call([&export_args](const std::string& s) {
             set_partitioning(s, &export_args.sample_partitioning);
           }),
       option("--region-partition") %
               "Partitions the list of regions to be exported and causes this "
               "export to export only a specific partition of them. Specify in "
               "the format I:N where I is the partition index and N is the "
               "total number of partitions. Useful for batch exports." &
           value("I:N").call([&export_args](const std::string& s) {
             set_partitioning(s, &export_args.region_partitioning);
           }),
       option("--upload-dir") %
               "If set, all output file(s) from the export process will be "
               "copied to the given directory (or S3 prefix) upon completion." &
           value("path", export_args.upload_dir),
       option("--tiledb-config") %
               "CSV string of the format 'param1=val1,param2=val2...' "
               "specifying optional TileDB configuration parameter settings." &
           value("params").call([&export_args](const std::string& s) {
             export_args.tiledb_config = utils::split(s, ',');
           }),
       option("-v", "--verbose").set(export_args.verbose) %
           "Enable verbose output",
       option("-c", "--count-only").call([&export_args]() {
         export_args.export_to_disk = false;
         export_args.cli_count_only = true;
       }) % "Don't write output files, only print the count of the resulting "
            "number of intersecting records.",
       option("-b", "--mem-budget-mb") %
               defaulthelp(
                   "The memory budget (MB) used when submitting TileDB "
                   "queries.",
                   export_args.memory_budget_mb) &
           value("MB", export_args.memory_budget_mb),
       ((option("-f", "--samples-file") %
             "Path to file with 1 sample name per line" &
         value("path", export_args.samples_file_uri)) |
        (required("-s", "--sample-names") %
             "CSV list of sample names to export" &
         value("samples").call([&](const std::string& s) {
           export_args.sample_names = utils::split(s, ',');
         }))),
       option("--stats").set(export_args.tiledb_stats_enabled) %
           "Enable TileDB stats");

  ListParams list_args;
  auto list_mode =
      (required("-u", "--uri") % "TileDB dataset URI" &
           value("uri", list_args.uri),
       option("--tiledb-config") %
               "CSV string of the format 'param1=val1,param2=val2...' "
               "specifying optional TileDB configuration parameter settings." &
           value("params").call([&list_args](const std::string& s) {
             list_args.tiledb_config = utils::split(s, ',');
           }));

  StatParams stat_args;
  auto stat_mode =
      (required("-u", "--uri") % "TileDB dataset URI" &
           value("uri", stat_args.uri),
       option("--tiledb-config") %
               "CSV string of the format 'param1=val1,param2=val2...' "
               "specifying optional TileDB configuration parameter settings." &
           value("params").call([&stat_args](const std::string& s) {
             stat_args.tiledb_config = utils::split(s, ',');
           }));

  auto cli =
      (command("--version", "-v", "version").set(opmode, Mode::Version) %
           "Prints the version and exits." |
       (command("create").set(opmode, Mode::Create), create_mode) |
       (command("register").set(opmode, Mode::Register), register_mode) |
       (command("store").set(opmode, Mode::Store), store_mode) |
       (command("export").set(opmode, Mode::Export), export_mode) |
       (command("list").set(opmode, Mode::List), list_mode) |
       (command("stat").set(opmode, Mode::Stat), stat_mode));

  if (!parse(argc, argv, cli)) {
    if (argc > 1) {
      // Try to print the right help page.
      if (std::string(argv[1]) == "create") {
        usage_create(create_mode);
      } else if (std::string(argv[1]) == "register") {
        usage_register(register_mode);
      } else if (std::string(argv[1]) == "store") {
        usage_store(store_mode);
      } else if (std::string(argv[1]) == "export") {
        usage_export(export_mode);
      } else if (std::string(argv[1]) == "list") {
        usage_list(list_mode);
      } else if (std::string(argv[1]) == "stat") {
        usage_stat(stat_mode);
      } else {
        usage(
            cli,
            create_mode,
            register_mode,
            store_mode,
            export_mode,
            list_mode,
            stat_mode);
      }
    } else {
      usage(
          cli,
          create_mode,
          register_mode,
          store_mode,
          export_mode,
          list_mode,
          stat_mode);
    }
    return 1;
  }

  switch (opmode) {
    case Mode::Version:
      std::cout << version_info() << "\n";
      break;
    case Mode::Create:
      do_create(create_args);
      break;
    case Mode::Register:
      do_register(register_args);
      break;
    case Mode::Store:
      do_store(store_args);
      break;
    case Mode::Export:
      do_export(export_args);
      break;
    case Mode::List:
      do_list(list_args);
      break;
    case Mode::Stat:
      do_stat(stat_args);
      break;
    default:
      usage(
          cli,
          create_mode,
          register_mode,
          store_mode,
          export_mode,
          list_mode,
          stat_mode);
      return 1;
  }

  return 0;
}
