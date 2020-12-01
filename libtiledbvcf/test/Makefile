#== dependencies
# - bgzip
# - tabix

#== bedfile
roadmap_url  := https://egg2.wustl.edu/roadmap/data/byFileType/chromhmmSegmentations/ChmmModels/coreMarks/jointModel/final

bed_file  := inputs/E001_15_coreMarks_dense_filtered.bed
bed_raw   := $(subst _filtered,,$(bed_file))
bed_gz    := $(addsuffix .gz, $(bed_file))
bed_index := $(addsuffix .tbi, $(bed_file))

#== main
.PHONY: clean
.INTERMEDIATE: $(bed_raw)

bedfile: $(bed_file) $(bed_index)

$(bed_index): $(bed_gz)
	@echo "Indexing $< ..."
	@tabix $<

$(bed_gz): $(bed_file)
	@echo "Compressing $< ..."
	@bgzip -c $< > $@

$(bed_file): $(bed_raw)
	@echo "Filtering regions from $<..."
	@grep -E 'chr(1|2|3)\t' $< \
		| awk -F'\t' 'BEGIN {OFS="\t"}; { \
				if ($$4 == "7_Enh") {print} \
			}' > $@

$(bed_raw):
	@echo "Downloading $@..."
	@curl --silent -o $(addsuffix .gz, $@) $(roadmap_url)/$(@F)
	@gzip -d $(addsuffix .gz, $@)

clean:
	rm -f $(bed_file) $(bed_index)
