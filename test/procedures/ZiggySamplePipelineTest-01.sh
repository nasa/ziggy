ZiggySamplePipelineTest-01() {
    pause "Perform steps 2 - 5 of ZiggySamplePipelineTest (1)" "verify the files in the datastore"

    # TODO Run the pipeline

    # Sanitize date stamps
    replacement='s/[0-9]\{4\}-[0-9]\{2\}-[0-9]\{2\}.0001-sample-model.txt/YYYY-MM-DD.0001-sample-model.txt/'
    sed $replacement $expected > /tmp/$test.expected

    # TODO Extract datastore directory from property file (repurpose ziggy dump-system-properties?)
    find sample-pipeline/build/pipeline-results/datastore | sort | sed $replacement > /tmp/$test.out
    diff -u /tmp/$test.expected /tmp/$test.out
    logTestResult $? "datastore content to match expected outputs"
    rm /tmp/$test.expected /tmp/$test.out
}
