<!-- -*-visual-line-*- -->

[[Previous]](system-requirements.md)
[[Up]](user-manual.md)
[[Next]](pipeline-architecture.md)

### Quick Start

Here's an extremely quick start to compile Ziggy and run a sample pipeline:

```
export ZIGGY_ROOT=/path/to/ziggy
export ZIGGY_HOME=$ZIGGY_ROOT/build
export PIPELINE_CONFIG_PATH=$ZIGGY_ROOT/sample-pipeline/etc/sample.properties 
export JAVA_HOME=/path/to/>=java-17
export CC=/path/to/gcc
export CXX=/path/to/g++
export PATH=$ZIGGY_HOME/bin:$JAVA_HOME/bin:`dirname $CC`:/path/to/python3>=3.8/bin:$PATH
./gradlew
sample-pipeline/build-env.sh
ziggy cluster init && ziggy cluster start && ziggy console start sample
while true; do ziggy console display --instance 1 --displayType full; sleep 10; done 
```
Hit C-c when the last command shows that the pipeline is in the COMPLETED state.

Be sure the USER environment variable is defined. We've found that Python 3.6 does not work, that Python 3.11 works, and that Python 3.8 probably works.

[[Previous]](system-requirements.md)
[[Up]](user-manual.md)
[[Next]](pipeline-architecture.md)
