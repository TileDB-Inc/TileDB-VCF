#!/bin/bash

#
# This file runs some TileDB-VCF CLI tests.
#

if [[ $# -lt 2 ]]; then
    echo "USAGE: $0 <build-dir> <inputs-dir>"
    exit 1
fi

build_dir=$1
input_dir=$2
tilevcf=${build_dir}/libtiledbvcf/src/tiledbvcf
upload_dir=/tmp/tilevcf-upload-dir-$$

# Clean up test outputs
function clean_up {
    rm -rf ingested_1 ingested_2 ingested_3 ingested_3_attrs \
           ingested_1_2 ingested_1_2_vcf ingested_3_samples ingested_comb ingested_append \
           ingested_from_file ingested_diff_order ingested_buffered \
           ingested_sep_indexes ingested_dupe_end_pos errored_dupe_end_pos \
           ingested_capacity HG01762.vcf HG00280.vcf tmp.bed tmp1.vcf tmp2.vcf \
           region-map.txt pfx.tsv
    rm -rf "$upload_dir"
}

# Simple helper function to create, register, and ingest a set of samples.
function create_register_ingest {
    local uri=$1
    shift
    $tilevcf create -u $uri || exit 1
    $tilevcf register -u $uri $@ || exit 1
    $tilevcf store -u $uri $@ || exit 1
}

# Clean up from previous runs
clean_up

mkdir $upload_dir

# Ingest
create_register_ingest ingested_1 ${input_dir}/small.bcf
create_register_ingest ingested_2 ${input_dir}/small2.bcf
create_register_ingest ingested_3 ${input_dir}/small3.bcf
create_register_ingest ingested_1_2 ${input_dir}/small2.bcf ${input_dir}/small.bcf
create_register_ingest ingested_1_2_vcf ${input_dir}/small2.bcf ${input_dir}/small.vcf.gz
create_register_ingest ingested_3_samples ${input_dir}/random_synthetic/G{1,2,3}.bcf
create_register_ingest ingested_dupe_end_pos ${input_dir}/dupeEndPos.vcf.gz

$tilevcf create -u ingested_3_attrs -a fmt_DP,info_MLEAC,info_MLEAF,info_MQ,fmt_AD,info_GQ || exit 1
$tilevcf register -u ingested_3_attrs ${input_dir}/small3.bcf || exit 1
$tilevcf store -u ingested_3_attrs ${input_dir}/small3.bcf || exit 1
$tilevcf create -u ingested_append -a fmt_DP,fmt_AD,info_GQ || exit 1
$tilevcf register -u ingested_append ${input_dir}/small.bcf ${input_dir}/small2.bcf || exit 1
$tilevcf store -u ingested_append ${input_dir}/small2.bcf || exit 1
$tilevcf store -u ingested_append ${input_dir}/small.bcf || exit 1
echo -e "${input_dir}/small.bcf\n${input_dir}/small2.bcf" > samples.txt
$tilevcf create -u ingested_from_file -a fmt_DP,fmt_AD,info_GQ || exit 1
$tilevcf register -u ingested_from_file -f samples.txt || exit 1
$tilevcf store -u ingested_from_file --remove-sample-file -f samples.txt || exit 1
test -e samples.txt && exit 1
$tilevcf create -u ingested_diff_order || exit 1
$tilevcf register -u ingested_diff_order ${input_dir}/small.bcf ${input_dir}/small2.bcf || exit 1
$tilevcf store -u ingested_diff_order ${input_dir}/small2.bcf ${input_dir}/small.bcf || exit 1
$tilevcf create -u ingested_buffered || exit 1
$tilevcf register -u ingested_buffered ${input_dir}/small.bcf ${input_dir}/small2.bcf || exit 1
$tilevcf store -u ingested_buffered -n 1 ${input_dir}/small2.bcf ${input_dir}/small.bcf || exit 1
$tilevcf create -u ingested_capacity -c 1 || exit 1
$tilevcf register -u ingested_capacity ${input_dir}/small.bcf ${input_dir}/small2.bcf || exit 1
$tilevcf store -u ingested_capacity ${input_dir}/small2.bcf ${input_dir}/small.bcf || exit 1
rm -f samples.txt
echo -e "${input_dir}/separate_indexes/small.bcf\t${input_dir}/separate_indexes/idx/small_index.csi" >> samples.txt
echo -e "${input_dir}/separate_indexes/small2.bcf\t${input_dir}/separate_indexes/idx/small2_index.csi" >> samples.txt
$tilevcf create -u ingested_sep_indexes -a fmt_DP,fmt_AD,info_GQ || exit 1
$tilevcf register -u ingested_sep_indexes -f samples.txt || exit 1
$tilevcf store -u ingested_sep_indexes --remove-sample-file -f samples.txt || exit 1
test -e samples.txt && exit 1

# Run export checks
$tilevcf export -u ingested_1 -s HG01762 -O v -b 512
diff <(bcftools view --no-version ${input_dir}/small.bcf) HG01762.vcf || exit 1
$tilevcf export -u ingested_2 -s HG00280 -O v -b 512
diff <(bcftools view --no-version ${input_dir}/small2.bcf) HG00280.vcf || exit 1
$tilevcf export -u ingested_1_2 -v -s HG01762,HG00280 -O v -b 512
diff -q <(bcftools view --no-version ${input_dir}/small.bcf) HG01762.vcf || exit 1
diff -q <(bcftools view --no-version ${input_dir}/small2.bcf) HG00280.vcf || exit 1
rm -f HG00280.vcf HG01762.vcf
$tilevcf export -u ingested_1_2_vcf -s HG01762,HG00280 -O v -b 512
diff -q <(bcftools view --no-version ${input_dir}/small.vcf.gz) HG01762.vcf || exit 1
diff -q <(bcftools view --no-version ${input_dir}/small2.bcf) HG00280.vcf || exit 1
rm -f HG00280.vcf HG01762.vcf
$tilevcf export -u ingested_diff_order -s HG01762,HG00280 -O v -b 512
diff -q <(bcftools view --no-version ${input_dir}/small.bcf) HG01762.vcf || exit 1
diff -q <(bcftools view --no-version ${input_dir}/small2.bcf) HG00280.vcf || exit 1
rm -f HG00280.vcf HG01762.vcf
$tilevcf export -u ingested_buffered -s HG01762,HG00280 -O v -b 512
diff -q <(bcftools view --no-version ${input_dir}/small.bcf) HG01762.vcf || exit 1
diff -q <(bcftools view --no-version ${input_dir}/small2.bcf) HG00280.vcf || exit 1
rm -f HG00280.vcf HG01762.vcf
$tilevcf export -u ingested_capacity -s HG01762,HG00280 -O v -b 512
diff -q <(bcftools view --no-version ${input_dir}/small.bcf) HG01762.vcf || exit 1
diff -q <(bcftools view --no-version ${input_dir}/small2.bcf) HG00280.vcf || exit 1
rm -f HG00280.vcf HG01762.vcf
$tilevcf export -u ingested_1_2 -v -s HG01762,HG00280 -O v -b 512
diff -q <(bcftools view --no-version ${input_dir}/small.bcf) HG01762.vcf || exit 1
diff -q <(bcftools view --no-version ${input_dir}/small2.bcf) HG00280.vcf || exit 1

## Check whole export for ingested_3 which has some indels, where we add END tags
## on export (which are not present in the input BCF). So we just compare without
## the END tags.
rm -f HG00280.vcf HG01762.vcf
$tilevcf export -u ingested_3 -v -s HG00280 -O v -b 512
diff -q <(bcftools annotate --no-version -x INFO/END ${input_dir}/small3.bcf) <(bcftools annotate --no-version -x INFO/END HG00280.vcf) || exit 1

## Run region export checks
rm -f HG00280.vcf HG01762.vcf
region="1:12141-15000"
$tilevcf export -u ingested_1_2 -r $region -v -s HG01762,HG00280 -O v -b 512
diff -q <(bcftools view --no-version -r $region ${input_dir}/small.bcf) HG01762.vcf || exit 1
diff -q <(bcftools view --no-version -r $region ${input_dir}/small2.bcf) HG00280.vcf || exit 1
rm -f HG00280.vcf HG01762.vcf
region="1:13300-13390,1:13400-13413,1:13452-13500,1:13600-17480"
$tilevcf export -u ingested_1_2 -r $region -v -s HG01762,HG00280 -O v -b 512
diff -q <(bcftools view --no-version -r $region ${input_dir}/small.bcf) HG01762.vcf || exit 1
diff -q <(bcftools view --no-version -r $region ${input_dir}/small2.bcf) HG00280.vcf || exit 1
rm -f HG00280.vcf HG01762.vcf
region="1:12100-12800,1:13500-17350"
$tilevcf export -u ingested_1_2 -r $region -v -s HG01762,HG00280 -O v -b 512
diff -q <(bcftools view --no-version -r $region ${input_dir}/small.bcf) HG01762.vcf || exit 1
diff -q <(bcftools view --no-version -r $region ${input_dir}/small2.bcf) HG00280.vcf || exit 1
rm -f HG00280.vcf HG01762.vcf
region="1\t12141\t15000"
echo -e "$region" > tmp.bed
$tilevcf export -u ingested_1_2 -R tmp.bed -v -s HG01762,HG00280 -O v -b 512
diff -q <(bcftools view --no-version -R tmp.bed ${input_dir}/small.bcf) HG01762.vcf || exit 1
diff -q <(bcftools view --no-version -R tmp.bed ${input_dir}/small2.bcf) HG00280.vcf || exit 1
rm -f HG00280.vcf HG01762.vcf
region="1\t12141\t15000\n1\t17484\t18000"
echo -e "$region" > tmp.bed
$tilevcf export -u ingested_1_2 -R tmp.bed -v -s HG01762,HG00280 -O v -b 512
diff -q <(bcftools view --no-version -R tmp.bed ${input_dir}/small.bcf) HG01762.vcf || exit 1
diff -q <(bcftools view --no-version -R tmp.bed ${input_dir}/small2.bcf) HG00280.vcf || exit 1

## Region export checks with indels
rm -f HG00280.vcf HG01762.vcf
region="1:12100-12800,1:13500-17350"
$tilevcf export -u ingested_3 -r $region -v -s HG00280 -O v -b 512
diff -q <(bcftools annotate --no-version -x INFO/END -r $region ${input_dir}/small3.bcf) <(bcftools annotate --no-version -x INFO/END HG00280.vcf) || exit 1
rm -f HG00280.vcf HG01762.vcf
region="1:70000-866511"
$tilevcf export -u ingested_3 -r $region -v -s HG00280 -O v -b 512
diff -q <(bcftools annotate --no-version -x INFO/END -r $region ${input_dir}/small3.bcf) <(bcftools annotate --no-version -x INFO/END HG00280.vcf) || exit 1
rm -f HG00280.vcf HG01762.vcf
region="1:1289365-1289368"
$tilevcf export -u ingested_3 -r $region -v -s HG00280 -O v -b 512
diff -q <(bcftools annotate --no-version -x INFO/END -r $region ${input_dir}/small3.bcf) <(bcftools annotate --no-version -x INFO/END HG00280.vcf) || exit 1
rm -f HG00280.vcf HG01762.vcf
region="1:1289370-1289370" # empty region
$tilevcf export -u ingested_3 -r $region -v -s HG00280 -O v -b 512
test -e HG00280.vcf && exit 1

# Region export checks with sample partitioning
rm -f HG00280.vcf HG01762.vcf
region="1:13300-13390,1:13400-13413,1:13452-13500,1:13600-17480"
$tilevcf export -u ingested_1_2 -r $region -v -s HG01762,HG00280 -O v --sample-partition 0:1 -b 512
diff -q <(bcftools view --no-version -r $region ${input_dir}/small.bcf) HG01762.vcf || exit 1
diff -q <(bcftools view --no-version -r $region ${input_dir}/small2.bcf) HG00280.vcf || exit 1
rm -f HG00280.vcf HG01762.vcf
region="1:13300-13390,1:13400-13413,1:13452-13500,1:13600-17480"
$tilevcf export -u ingested_1_2 -r $region -v -s HG01762,HG00280 -O v --sample-partition 0:2 -b 512
test -e HG01762.vcf && exit 1
diff -q <(bcftools view --no-version -r $region ${input_dir}/small2.bcf) HG00280.vcf || exit 1
rm -f HG00280.vcf HG01762.vcf
region="1:13300-13390,1:13400-13413,1:13452-13500,1:13600-17480"
$tilevcf export -u ingested_1_2 -r $region -v -s HG00280,HG01762 -O v --sample-partition 0:2 -b 512
test -e HG01762.vcf && exit 1
diff -q <(bcftools view --no-version -r $region ${input_dir}/small2.bcf) HG00280.vcf || exit 1
rm -f HG00280.vcf HG01762.vcf
region="1:13300-13390,1:13400-13413,1:13452-13500,1:13600-17480"
$tilevcf export -u ingested_1_2 -r $region -v -s HG01762,HG00280 -O v --sample-partition 1:2 -b 512
test -e HG00280.vcf && exit 1
diff -q <(bcftools view --no-version -r $region ${input_dir}/small.bcf) HG01762.vcf || exit 1

# Region export checks with output dir
rm -f HG00280.vcf HG01762.vcf
rm -f /tmp/HG00280.vcf /tmp/HG01762.vcf
region="1:13300-13390,1:13400-13413,1:13452-13500,1:13600-17480"
$tilevcf export -u ingested_1_2 -r $region -v -s HG01762,HG00280 -O v -d /tmp -b 512
test -e HG01762.vcf && exit 1
test -e HG00280.vcf && exit 1
test -e /tmp/HG01762.vcf || exit 1
test -e /tmp/HG00280.vcf || exit 1
diff -q <(bcftools view --no-version -r $region ${input_dir}/small.bcf) /tmp/HG01762.vcf || exit 1
diff -q <(bcftools view --no-version -r $region ${input_dir}/small2.bcf) /tmp/HG00280.vcf || exit 1
rm -f /tmp/HG00280.vcf /tmp/HG01762.vcf

# Region export checks with upload dir
rm -f HG00280.vcf HG01762.vcf
rm -f /tmp/HG00280.vcf /tmp/HG01762.vcf
region="1:13300-13390,1:13400-13413,1:13452-13500,1:13600-17480"
$tilevcf export -u ingested_1_2 -r $region -v -s HG01762,HG00280 -O v -d /tmp --upload-dir $upload_dir -b 512
test -e HG01762.vcf && exit 1
test -e HG00280.vcf && exit 1
test -e /tmp/HG01762.vcf || exit 1
test -e /tmp/HG00280.vcf || exit 1
test -e ${upload_dir}/HG01762.vcf || exit 1
test -e ${upload_dir}/HG00280.vcf || exit 1
diff -q <(bcftools view --no-version -r $region ${input_dir}/small.bcf) /tmp/HG01762.vcf || exit 1
diff -q <(bcftools view --no-version -r $region ${input_dir}/small2.bcf) /tmp/HG00280.vcf || exit 1
diff -q <(bcftools view --no-version -r $region ${input_dir}/small.bcf) ${upload_dir}/HG01762.vcf || exit 1
diff -q <(bcftools view --no-version -r $region ${input_dir}/small2.bcf) ${upload_dir}/HG00280.vcf || exit 1
rm -f /tmp/HG00280.vcf /tmp/HG01762.vcf ${upload_dir}/*

# Check TSV output
rm -f HG00280.vcf HG01762.vcf region-map.txt
region="1\t12141\t15000\n1\t17484\t18000"
echo -e "$region" > tmp.bed
$tilevcf export -u ingested_1_2 -R tmp.bed -O t -o pfx.tsv -t CHR,POS,I:END,REF,ALT,FILTER -v -s HG01762,HG00280 -b 512 || exit 1
diff -wq pfx.tsv <(
cat <<EOF
SAMPLE	CHR	POS	I:END	REF	ALT	FILTER
HG00280	1	12141	12277	C	<NON_REF>
HG01762	1	12141	12277	C	<NON_REF>
HG00280	1	12546	12771	G	<NON_REF>
HG01762	1	12546	12771	G	<NON_REF>
HG00280	1	13354	13374	T	<NON_REF>	LowQual
HG01762	1	13354	13389	T	<NON_REF>
HG00280	1	13375	13395	G	<NON_REF>
HG00280	1	13396	13413	T	<NON_REF>
HG00280	1	13414	13451	C	<NON_REF>
HG00280	1	13452	13519	G	<NON_REF>
HG00280	1	13520	13544	G	<NON_REF>
HG00280	1	13545	13689	G	<NON_REF>
HG00280	1	17480	17486	A	<NON_REF>
EOF
) || exit 1
rm -f HG00280.vcf HG01762.vcf region-map.txt /tmp/pfx.tsv
region="1\t12141\t15000\n1\t17484\t18000"
echo -e "$region" > tmp.bed
$tilevcf export -u ingested_1_2 -R tmp.bed -O t -o pfx.tsv -t CHR,POS,I:END,REF,ALT,FILTER -v -s HG01762,HG00280 -d /tmp/ -b 512 || exit 1
diff -wq /tmp/pfx.tsv <(
cat <<EOF
SAMPLE	CHR	POS	I:END	REF	ALT	FILTER
HG00280	1	12141	12277	C	<NON_REF>
HG01762	1	12141	12277	C	<NON_REF>
HG00280	1	12546	12771	G	<NON_REF>
HG01762	1	12546	12771	G	<NON_REF>
HG00280	1	13354	13374	T	<NON_REF>	LowQual
HG01762	1	13354	13389	T	<NON_REF>
HG00280	1	13375	13395	G	<NON_REF>
HG00280	1	13396	13413	T	<NON_REF>
HG00280	1	13414	13451	C	<NON_REF>
HG00280	1	13452	13519	G	<NON_REF>
HG00280	1	13520	13544	G	<NON_REF>
HG00280	1	13545	13689	G	<NON_REF>
HG00280	1	17480	17486	A	<NON_REF>
EOF
) || exit 1
rm -f /tmp/pfx.tsv
rm -f HG00280.vcf HG01762.vcf region-map.txt $upload_dir/*
region="1\t12141\t15000\n1\t17484\t18000"
echo -e "$region" > tmp.bed
$tilevcf export -u ingested_1_2 -R tmp.bed -O t -o pfx.tsv -t CHR,POS,I:END,REF,ALT,FILTER -v -s HG01762,HG00280 --upload-dir $upload_dir -b 512 || exit 1
diff -wq $upload_dir/pfx.tsv <(
cat <<EOF
SAMPLE	CHR	POS	I:END	REF	ALT	FILTER
HG00280	1	12141	12277	C	<NON_REF>
HG01762	1	12141	12277	C	<NON_REF>
HG00280	1	12546	12771	G	<NON_REF>
HG01762	1	12546	12771	G	<NON_REF>
HG00280	1	13354	13374	T	<NON_REF>	LowQual
HG01762	1	13354	13389	T	<NON_REF>
HG00280	1	13375	13395	G	<NON_REF>
HG00280	1	13396	13413	T	<NON_REF>
HG00280	1	13414	13451	C	<NON_REF>
HG00280	1	13452	13519	G	<NON_REF>
HG00280	1	13520	13544	G	<NON_REF>
HG00280	1	13545	13689	G	<NON_REF>
HG00280	1	17480	17486	A	<NON_REF>
EOF
) || exit 1
rm -f /tmp/pfx.tsv
rm -f HG00280.vcf HG01762.vcf region-map.txt $upload_dir/*

echo "Export non-contiguous samples (#79)"
$tilevcf export -u ingested_3_samples -Ob -v -s G1,G3 || exit 1
rm -f G{1,3}.bcf

# Check count only
region="1\t12141\t15000\n1\t17484\t18000"
echo -e "$region" > tmp.bed
diff -wq <(echo 13) <($tilevcf export -u ingested_1_2 -R tmp.bed -c -s HG01762,HG00280) || exit 1

# Check TSV output with query range columns
rm -f HG00280.vcf HG01762.vcf
region="1\t12141\t15000\n1\t17484\t18000"
echo -e "$region" > tmp.bed
$tilevcf export -u ingested_1_2 -R tmp.bed -O t -o pfx.tsv -t CHR,POS,I:END,REF,Q:POS,Q:END -v -s HG01762,HG00280 || exit 1
diff -wq pfx.tsv <(
cat <<EOF
SAMPLE	CHR	POS	I:END	REF	Q:POS	Q:END
HG00280	1	12141	12277	C	12142	15000
HG01762	1	12141	12277	C	12142	15000
HG00280	1	12546	12771	G	12142	15000
HG01762	1	12546	12771	G	12142	15000
HG00280	1	13354	13374	T	12142	15000
HG01762	1	13354	13389	T	12142	15000
HG00280	1	13375	13395	G	12142	15000
HG00280	1	13396	13413	T	12142	15000
HG00280	1	13414	13451	C	12142	15000
HG00280	1	13452	13519	G	12142	15000
HG00280	1	13520	13544	G	12142	15000
HG00280	1	13545	13689	G	12142	15000
HG00280	1	17480	17486	A	17485	18000
EOF
) || exit 1
rm -f HG00280.vcf HG01762.vcf region-map.txt /tmp/pfx.tsv

# Check multiple register/store stages
rm -rf ingested_1_2 HG00280.vcf HG01762.vcf
$tilevcf create -u ingested_1_2 || exit 1
$tilevcf register -u ingested_1_2 ${input_dir}/small2.bcf || exit 1
$tilevcf store -u ingested_1_2 ${input_dir}/small2.bcf || exit 1
$tilevcf register -u ingested_1_2 ${input_dir}/small.bcf || exit 1
$tilevcf store -u ingested_1_2 ${input_dir}/small.bcf || exit 1
$tilevcf export -u ingested_1_2 -s HG01762,HG00280 -O v
diff -q <(bcftools view --no-version ${input_dir}/small.bcf) HG01762.vcf || exit 1
diff -q <(bcftools view --no-version ${input_dir}/small2.bcf) HG00280.vcf || exit 1

# Check sample listing
diff -q <($tilevcf list -u ingested_1) <(
cat <<EOF
HG01762
EOF
) || exit 1
diff -q <($tilevcf list -u ingested_1_2) <(
cat <<EOF
HG00280
HG01762
EOF
) || exit 1

# Check stat command
diff -q <($tilevcf stat -u ingested_1_2) <(
cat <<EOF
Statistics for dataset 'ingested_1_2':
- Version: 2
- Row tile extent: 10
- Tile capacity: 10,000
- Anchor gap: 1,000
- Number of registered samples: 2
- Extracted attributes: none
EOF
) || exit 1

# Expected failures
echo ""
echo "** Expected failure error messages follow:"
$tilevcf create -u ingested_comb || exit 1
$tilevcf register -u ingested_comb ${input_dir}/small_combined.bcf && exit 1
rm -f HG00280.vcf HG01762.vcf
$tilevcf export -u ingested_1_2 -s HG01762,HG00280 -O v || exit 1
diff -q <(bcftools view --no-version ${input_dir}/small.bcf) HG00280.vcf && exit 1
diff -q <(bcftools view --no-version ${input_dir}/small2.bcf) HG01762.vcf && exit 1
rm -f HG00280.vcf HG01762.vcf
$tilevcf export -u ingested_1_2 -r "1:13300-13390" -v -s HG00280 -O v --sample-partition 0:2 && exit 1
$tilevcf export -u ingested_1_2 -r "1:13300-13390" -v -s HG00280 -O v --sample-partition 1:1 && exit 1
$tilevcf export -u ingested_1_2 -r "1:13300-13390" -v -s HG00280 -O v --sample-partition 2:1 && exit 1
$tilevcf export -u ingested_1_2 -r "1:13300-13390" -v -s HG01762,HG00280 -O v --sample-partition 0:3 && exit 1
$tilevcf create -u ingested_bad -a GT && exit 1
# expect duplicate coordinates error
$tilevcf create -u errored_dupe_end_pos --no-duplicates
$tilevcf register -u errored_dupe_end_pos ${input_dir}/dupeEndPos.vcf.gz
$tilevcf store -u errored_dupe_end_pos ${input_dir}/dupeEndPos.vcf.gz
echo "** End expected error messages."

# Clean up
clean_up

echo ""
echo "VCF verification passed."
exit 0