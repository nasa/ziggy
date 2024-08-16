#!/usr/bin/perl

use warnings;
use strict;

use Data::Dumper;
use File::Basename;
use File::Spec;
use Getopt::Long qw(:config pass_through);
use Getopt::Long qw(HelpMessage);

my @options = qw{
    dry-run
    properties=s
    verbose!
    ziggyHome=s
};

my %optionValues = (
    "dry-run"      => 0,
    "properties"   => $ENV{'PIPELINE_CONFIG_PATH'},
    "verbose"      => 0,
    "ziggyHome"    => '',               # default is ziggy.home.dir property
);

my %properties;

my $exitCode;
eval {
    $exitCode = main();
};
if (my $err = $@) {
    print STDERR "# $0: $_\n", foreach split /\n/, $err;
    exit 1;
}
exit ($exitCode != 0);

sub main {
    # Load user options, first picking out what's in %optionValues. Do not allow
    # abbreviated options for %optionValues, and pass the rest into @ARGV.
    GetOptions(\%optionValues, @options);
    my ($nickname, $className, $ziggyHelp, $jvmOptions, $programOptions) = getUserOptions(@ARGV);

    if ($ziggyHelp) {
        HelpMessage();
    }

    # Read user-defined properties.
    defined($optionValues{properties})
        or die "Properties file not specified with either PIPELINE_CONFIG_PATH or properties option";
    %properties = getProperties($optionValues{properties}, %properties);

    # Determine ziggyHome from option or ziggy.home.dir property.
    if ($optionValues{ziggyHome} eq "") {
        exists($properties{'ziggy.home.dir'})
            or die "Missing Ziggy property ziggy.home.dir";
        $optionValues{ziggyHome} = makeSubstitutions($properties{'ziggy.home.dir'}, %properties);
    }
    -d $optionValues{ziggyHome}
        or die "Directory $optionValues{ziggyHome} set by ziggyHome option or ziggy.home.dir property does not exist";

    # Read Ziggy properties.
    my $ziggyProperties = File::Spec->catfile($optionValues{ziggyHome}, "etc", "ziggy.properties");
    %properties = getProperties($ziggyProperties, %properties);

    # Extract nicknames from both property files.
    my %nicknames = getNicknames(%properties);

    # If the user didn't specify a nickname or fully qualified class name,
    # display the nicknames.
    if (!(defined($nickname) or defined($className))) {
        displayNicknames(%nicknames);
        return 0;
    }

    # Update unset environment variables from options.
    if (!defined($ENV{'ZIGGY_HOME'})) {
        $ENV{'ZIGGY_HOME'} = $optionValues{ziggyHome};
    }
    if (!defined($ENV{'PIPELINE_CONFIG_PATH'})) {
        $ENV{'PIPELINE_CONFIG_PATH'} = $optionValues{properties};
    }

    # Make the Java runtime executable command. Use the location specified as
    # java.home in the properties file, if any; otherwise, assume that a
    # usable Java runtime executable is available on the search path.
    my $javacmd = 'java';
    my $javahome = "";
    if (defined($properties{'java.home'})) {
        $javahome = makeSubstitutions($properties{'java.home'}, %properties);
    }
    if ($javahome ne "") {
        $javacmd = File::Spec->catfile($javahome, 'bin', $javacmd);
        -e $javacmd
            or die "$javacmd executable does not exist";
    }

    # Start to make the command line.
    my $cmd = "$javacmd";
    my $pipelineClasspath = $properties{'ziggy.pipeline.classpath'};
    if (defined($pipelineClasspath) && $pipelineClasspath ne "") {
        $cmd .= " -cp \"$pipelineClasspath\"";
    } else {
        # Use Ziggy's libs as default.
        $cmd .= " -cp \"$optionValues{ziggyHome}/libs/*\"";
    }

    # If the user-specified name is one of the pre-defined entries in the
    # nicknames, use information that that nickname maps to. If not, the user
    # will need to specify everything.

    # Convert program options to a string, preserving empty options ("").
    my $programOptionsString = "";
    foreach (@$programOptions) {
        if ($programOptionsString ne "") {
            $programOptionsString .= " ";
        }
        $programOptionsString .= /^$/ ? '""' : $_;
    }

    if (defined($nickname)) {
        if (exists $nicknames{$nickname}) {
            my $substJvm = makeSubstitutions("$cmd $nicknames{$nickname}{jvmOptions} @$jvmOptions", %properties);
            $cmd = "$substJvm " .
                "$nicknames{$nickname}{className} " .
                "$nicknames{$nickname}{programOptions} $programOptionsString";
        } else {
            print "Nickname $nickname unknown\n\n";
            displayNicknames(%nicknames);
            return 0;
        }
    } elsif (defined($className)) {
        my $substJvm = makeSubstitutions("$cmd @$jvmOptions", %properties);
        $cmd = "$substJvm $className $programOptionsString";
    } else {
        print "Neither nickname nor class name provided";
        return 1;
    }

    my $dryRunText = $optionValues{"dry-run"} ? " (DRY-RUN)" : "";
    print "Running command$dryRunText:\n\t$cmd\n" if $optionValues{verbose} || $optionValues{"dry-run"};

    my $exitStatus = $optionValues{"dry-run"} ? 0 : system $cmd;
    if ($exitStatus) {
        my $program = defined($nickname) ? $nickname : $className;
        print "$program exited with status $exitStatus\n\n";
    }

    return $exitStatus;
}

# Parse through command line options. The first option without a leading dash
# is the nickname. All options before that or a --class option are JVM
# options, all options after that are program options.
sub getUserOptions {
    my @cmdLineOptions = @_;

    my $name  = undef;
    my $class  = undef;
    my $ziggyHelp = 0;
    my $jvmOptions  = [];
    my $programOptions = [];

    # Begin with the assumption that the Java class name/nickname has not been
    # found yet.
    my $isNameFound = 0;
    foreach (@cmdLineOptions) {
        # The Java class name or nickname is the first option that does not
        # have a leading dash.
        if (!$isNameFound) {
            if (!/^-/) {
                $isNameFound = 1;
                $name = $_;
                next;
            } elsif (/^--class=([[:alnum:].]+)/) {
                $isNameFound = 1;
                $class = $1;
                next;
            }
        }

        # The user is asking for help.
        if (!$isNameFound && (/^--h/ || /^-h/)) {
            $ziggyHelp = 1;
            next;
        }

        # If the name has been found already, all subsequent options are
        # program options. Program options that contain spaces were entered by
        # the user with quotes around them, and the quotes were stripped by
        # the shell. Add the quotes back in so the user's intent is preserved.
        #
        # Otherwise, they are JVM options.
        if ($isNameFound) {
            $_ = "\"$_\"" if / /;
            push @$programOptions, $_;
        } else {
            push @$jvmOptions, $_;
        }
    }

    return($name, $class, $ziggyHelp, $jvmOptions, $programOptions);
}

# Parses a Java properties file and return the properties found. Only
# lines of the form "var = value" are parsed. Properties escapes are not
# currently processed.
sub getProperties {
    my ($propertiesFile, %properties) = @_;

    open(my $fh, $propertiesFile)
        or die "Can't open properties file $propertiesFile";

    while (<$fh>) {
        s/#.*//;        # strip comments
        s/^\s+|\s+$//g; # strip leading/trailing whitespace
        chomp;          # strip EOL \n
        next if /^$/;   # ignore empty lines

        # Property names are any characters except ':' or '=', unless escaped.
        # Property names and values may use '\' to escape characters. Names
        # and values are separated by ':' or '='. See Properties#load(Reader)
        # for details. This code does not support continuation lines.
        if (/^(([^ =:]|\\.)+)\s*[=:]\s*(.*)$/) {
            my $key = $1;
            my $value = $3;
            $key =~ s/\\(.)/$1/g;
            $value =~ s/\\(.)/$1/g;
            $properties{$key} = $value;
        }
    }

    close $fh or warn "Error closing properties file $propertiesFile";

    return %properties;
}

# Substitute property value and environment variable references in a string.
sub makeSubstitutions {
    my ($s, %properties) = @_;
    my $substitutionMade = 1;           # to get the loop started

    while ($substitutionMade) {
        $substitutionMade = 0;

        # Substitute Apache Commons Configuration environment variable references.
        # Do this before the property reference to keep the latter from failing.
        if ($s =~ /\$\{env:([^}]+)}/) {
            my $envVar = $1;
            die "Missing environment variable '$envVar'" if (!defined($ENV{$envVar}));
            $s =~ s/\$\{env:$envVar}/$ENV{$envVar}/;
            $substitutionMade = 1;
        }

        # Substitute property references.
        if ($s =~ /\$\{([^}]+)}/) {
            my $key = $1;
            exists($properties{$key})
                or die "Missing Ziggy property '$key'";
            $s =~ s/\$\{$key}/$properties{$key}/;
            $substitutionMade = 1;
        }

        # Substitute environment variable references.
        if ($s =~ /\%([^%]+)\%/) {
            my $envVar = $1;
            die "Missing environment variable '$envVar'" if (!defined($ENV{$envVar}));
            $s =~ s/\%$envVar\%/$ENV{$envVar}/;
            $substitutionMade = 1;
        }
    }

    return $s;
}

# The nicknames table is a hash table, hash-keys being the nicknames of the
# Java programs, and the hash-values being an anonymous hash table of class
# name, log file basename, JVM options, and program options:
#
#       nickname => {
#           className => class name
#           logFile => basename for log file
#           jvmOptions   => JVM options
#           programOptions  => program options
#       }
#
# See the description for the property format that is used to build this
# structure.
sub getNicknames {
    my (%properties) = @_;
    my %nicknames = ();
    my $default_jvm_options = exists($properties{'ziggy.default.jvm.args'}) ? $properties{'ziggy.default.jvm.args'} . " " : "";

    foreach my $property (keys %properties) {
        next if ($property !~ /^ziggy\.nickname\./);

        # Turn the pipe-deliminated value into an array and ensure array is
        # fully populated, even if the input is not.
        my @fields = split(/\|/, makeSubstitutions($properties{$property}, %properties));
        push @fields, "" while scalar @fields < 4;

        # Load the data into the nicknames structure.
        my $nickname = $property =~ s/^ziggy\.nickname\.//r;
        $nicknames{$nickname}{className} = $fields[0];
        $nicknames{$nickname}{logFile} = $fields[1];
        $nicknames{$nickname}{jvmOptions} = $default_jvm_options . logFileOption($fields[1]) . " " . $fields[2];
        $nicknames{$nickname}{programOptions} = $fields[3];
    }

    return %nicknames;
}

sub logFileOption {
    my ($logFileBasename) = @_;
    my ($logFileName, $logFileOption);

    exists($properties{'ziggy.pipeline.results.dir'})
        or die "Missing Ziggy property ziggy.pipeline.results.dir";

    $logFileBasename = "ziggy" if $logFileBasename eq "";
    $logFileName = File::Spec->catfile($properties{'ziggy.pipeline.results.dir'}, 'logs', 'cli', $logFileBasename . '.log');
    $logFileOption = "-Dziggy.logFile=" . $logFileName;
    $logFileOption = "-Dziggy.logFile=/dev/null" if $logFileBasename eq "/dev/null";

    return $logFileOption;
}

# Displays the nicknames along with their fully qualified class names.
sub displayNicknames {
    my (%nicknames) = @_;

    my $max_key_length = maxKeyLength(%nicknames);
    my $format = "%-" . $max_key_length . "s  %s\n";
    printf($format, "NICKNAME", "CLASS NAME");
    foreach my $key (sort keys %nicknames) {
        printf($format, $key, $nicknames{$key}{className});
    }
}

sub maxKeyLength {

    my (%hash) = @_;
    my $max_length = -1;
    foreach my $key (keys %hash) {
        my $key_length = length $key;
        if ($key_length > $max_length) {
            $max_length = length $key;
        }
    }
    return $max_length;
}

=head1 NAME

ziggy - Run Ziggy tools

=head1 SYNOPSIS

ziggy [--help] [--dry-run] [--[no]verbose] [--ziggyHome=<dir>] [--properties=<file>] [<JVM options>] [<nickname> | --class=<fully qualified class name>] [<program options>]

=head1 OPTIONS

=over

=item --class=fully qualified class name

Run the Java class rather than a nickname.

=item --help

Prints a short help message.

=item --dry-run

Show the Java command that would be run, but do not run it. Default is off.

=item --[no]verbose

Print the Java command line as it is being run. Default is non-verbose.

=item --ziggyHome=directory

Specify the home directory of the package distribution. Default is the value
of the ziggy.home.dir property.

=item --properties=file

Specify the path to the properties file containing the Ziggy configuration.
Default is $PIPELINE_CONFIG_PATH.

=back

=head1  DESCRIPTION

A wrapper for the Ziggy Java tools, ziggy allows the user to run a subset of
the Java tools without having to specify any of the default JVM or program
options. Any Ziggy Java tool can be run if the desired JVM and program options
are specified. Additional JVM or program options can be added to either type
of Ziggy Java tool.

This wrapper makes use of properties found in the $PIPELINE_CONFIG_PATH
environment variable or in the --properties option. It also uses properties
found in a file called etc/ziggy.properties located in $ziggy.home.dir or the
--ziggyHome option. The properties that begin with ziggy.nickname define a
tool's nickname as follows:

    ziggy.nickname.<nickname> = <fully qualified class name>|<logfile basename>|<space-delimited JVM args>|<space-delimited program args>

Log files are found in logs/cli/<logfile basename>.log in pipeline
results directory. If a logfile basename is not specified, "ziggy"
is used.

Values can refer to other properties.

The contents of the property ziggy.default.jvm.args is added to the JVM
arguments.

If the user-specified name is a known nickname, the settings in the property's
value are used. Otherwise, the user must specify the fully qualified class
name with the --class option and all necessary parameters.

Additional options can be given on the command line. JVM options can be given
before the nickname or class option, and must have a '-' prefix. Program
options can be given after the nickname or class option, and do not require a
prefix. The nickname is taken to be the first option to ziggy without a '-'
prefix. The ziggy options --[no]verbose, --dry-run, --conffile, and
--ziggyHome are not passed on to the Java call.

=head1 AUTHORS

Kester Allen
Bill Wohler
PT

=cut
