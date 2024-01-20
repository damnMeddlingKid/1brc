source "$HOME/.sdkman/bin/sdkman-init.sh"
sdk use java 21.0.1-graal 1>&2

# ./mvnw clean verify removes target/ and will re-trigger native image creation.
if [ ! -f target/CalculateAverage_ericxiao_image ]; then
    NATIVE_IMAGE_OPTS="--gc=epsilon -O3 -march=native --enable-preview"
    # Use -H:MethodFilter=CalculateAverage_thomaswue.* -H:Dump=:2 -H:PrintGraph=Network for IdealGraphVisualizer graph dumping.
    native-image $NATIVE_IMAGE_OPTS -cp target/average-1.0.0-SNAPSHOT.jar -o target/CalculateAverage_ericxiao_image dev.morling.onebrc.CalculateAverage_ericxiao
fi