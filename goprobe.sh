#!/bin/sh

function error_exit() {
  local line_num="$1"
  local msg="$2"
  local err_cd="${3:-1}"
  if [[ -n "$msg" ]]
  then
    echo "Error on or near line ${line_num}: ${msg}; exiting with status ${err_cd}"
  else
    echo "Error on or near line ${line_num}; exiting with status ${err_cd}"
  fi
  exit "${err_cd}"
}

trap 'error_exit ${LINENO}' ERR

#define static variables
USER=`whoami`
HOME_DIR=`pwd`
CURR_DT=`date +%Y``date +%m``date +%d`
CURR_MT=`date +%Y``date +%m`
PROBES_DIR=$HOME_DIR/probes
OUT_DIR=$HOME_DIR/out
LOG_DIR=$HOME_DIR/log

#presto variables
_cli='/usr/bin/presto-cli.jar'
_server='http://presto-coordinator-new.synapse:3400'
_catalog='hive'
_schema='default'
_o_format='CSV_HEADER'

function analyze()
{
awk -F, -v k=${2:?} -v m=${3:?} '
  	BEGIN {
	  split(k,keys)
	  for (i in keys) k_offsets[keys[i]]=i
	  split(m,metrics)
	  for (i in metrics) m_offsets[metrics[i]]=i
	  printf("%s %s %s \n", key, row_count, average, std_dev)
	}
	NR == 1 {
	  for (f=1; f<=NF; f++)
	  {
	    gsub("\"","",$f)
	    if ($f in k_offsets) k_offsets[$f]=f
	    else if ($f in m_offsets) m_offsets[$f]=f
	    else c_offsets[$f]=f
	  }
	}
	NR != 1 {
	  super_key=""
	  for (k in k_offsets)
	    super_key = $k_offsets[k]"-"
	  for (m in m_offsets) 
	  {
	    gsub("\"","",$m_offsets[m])
	    key_name = super_key m
	    key_sum[key_name]+=$m_offsets[m]; 
	    key_sumsq[key_name]+=($m_offsets[m])^2
	    key_chain[key_name]++
	  }
	} 
	END {
	  for (key in key_chain){       
	    printf("%s %d %.2f %.2f \n", key, key_chain[key], key_sum[key]/key_chain[key], sqrt((key_sumsq[key]-key_sum[key]^2/key_chain[key])/key_chain[key]))
	  }
	}
' $1
}

function run_probe {
  probe=$1
  probe_name=`basename $probe`
  echo "Running probe $probe_name"
  java -jar ${_cli} --server ${_server} --catalog ${_catalog} --schema ${_schema} --output-format ${_o_format} --file $probe 1>$OUT_DIR/$probe_name-$CURR_DT.out 2>$LOG_DIR/$probe_name-$CURR_DT.log
  if [[ `find $LOG_DIR -empty -name $probe_name-$CURR_DT.log` == '' ]]
  then
    return 1
  else
  	analyze $OUT_DIR/$probe_name-$CURR_DT.out  
    return 0
  fi
}

for probe in $PROBES_DIR/*.pr
do
  run_probe $probe
  ret=$?
  if [[ ret -ne 0 ]]
  then
    echo "Error $ret running $probe"
  else
    echo "Completed probe $probe"
