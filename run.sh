SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

if [ -z ${1} ];
then
  READ_PATH="homework"
else
  READ_PATH=${1}
fi

if [ -z ${2}  ]; then
  OUTPUT_PATH="homework/result"
else
  OUTPUT_PATH=${2}
fi

echo READ_PATH=$READ_PATH OUTPUT_PATH=$OUTPUT_PATH

spark-submit \
$SCRIPT_DIR/main.py \
-r ${READ_PATH} \
-o ${OUTPUT_PATH}
