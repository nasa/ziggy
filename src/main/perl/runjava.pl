#!/usr/bin/perl

use warnings;
use strict;

use Data::Dumper;
use File::Basename;
use File::Spec;
use Getopt::Long qw(:config auto_help pass_through);

my %pars = (
    conffile     => '',                 # default is runjavaHome/etc/runjava.conf
    norun        => 0,
    properties   => $ENV{'PIPELINE_CONFIG_PATH'},
    runjavaHome  => '',                 # default is ziggy.home.dir property
    verbose      => 0,
);

my @args = qw{
    conffile=s
    norun
    properties=s
    runjavaHome=s
    verbose!
};

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
    # Load user arguments, first picking out what's in %pars. Do not allow
    # abbreviated args for %pars, and pass the rest into @ARGV.
    GetOptions(\%pars, @args);
    my ($inName, $jvmargs, $progargs) = getUserArgs(@ARGV);

    # Read properties.
    %properties = getProperties();

    # Determine runjavaHome from option or ziggy.home.dir property.
    if ($pars{runjavaHome} eq "") {
        exists($properties{'ziggy.home.dir'})
            or die "Missing Ziggy property ziggy.home.dir";
        $pars{runjavaHome} = makeSubstitutions($properties{'ziggy.home.dir'}, %properties);
    }
    -d $pars{runjavaHome}
        or die "Directory $pars{runjavaHome} set by runjavaHome option or ziggy.home.dir property does not exist";

    # Now that we have a home, find the configuration file.
    if ($pars{conffile} eq "") {
        $pars{conffile} = "$pars{runjavaHome}/etc/runjava.conf";
    }
    -e $pars{conffile}
        or die "$pars{conffile}: No such file";

    # If the user didn't specify a nickname or fully qualified class name,
    # display the nicknames.
    if (!(defined $inName and length $inName)) {
        displayNicknames();
        exit 0;
    }

    my %nicknames  = getNicknames(1);

    # Update unset environment variables from options.
    if (!defined($ENV{'ZIGGY_HOME'})) {
        $ENV{'ZIGGY_HOME'} = $pars{runjavaHome};
    }
    if (!defined($ENV{'PIPELINE_CONFIG_PATH'})) {
        $ENV{'PIPELINE_CONFIG_PATH'} = $pars{properties};
    }

    # Start to make the command line.  Every call will use this prefix:
    my $cmd = "java -cp \"$pars{runjavaHome}/libs/*";
    my $pipelineClasspath = $properties{'pipeline.classpath'};
    if (defined($pipelineClasspath) && $pipelineClasspath ne "") {
        $cmd .= ":" . $pipelineClasspath;
    }
    $cmd .= "\" ";

    # If the user-specified name is one of the pre-defined entries in the
    # nicknames, use information that that nickname maps to. If not, the user will
    # need to specify everything.
    if (exists $nicknames{$inName}) {
        my $substJvm = makeSubstitutions("$cmd $nicknames{$inName}{jvmargs} @$jvmargs", %properties);
        $cmd = "$substJvm " .
                "$nicknames{$inName}{classname} " .
                "$nicknames{$inName}{progargs} @$progargs";
    } else {
        my $substJvm = makeSubstitutions("$cmd @$jvmargs", %properties);
        $cmd = "$substJvm $inName @$progargs";
    }

    print "Running command:\n\t$cmd\n" if $pars{verbose};
    return system $cmd if not $pars{norun};
}

sub getUserArgs {
    # Parse through command line args. The first argument without a leading
    # dash is the classname/nickname. All args before that are JVM args, all
    # args after that are program args.
    my @cmdLineArgs = @_;

    my $name  = undef;
    my $jvms  = [];
    my $progs = [];

    # Begin with the assumption that the Java classname/nickname has not been
    # found yet.
    my $isNameFound = 0;
    foreach (@cmdLineArgs) {
        # The Java classname or nickname is the first argument that does not
        # have a leading dash.
        if (!$isNameFound && !/^-/) {
            $isNameFound = 1;
            $name = $_;
            next;
        }

        # If the name has been found already, all subsequent args are program
        # arguments. Program arguments that contain spaces were entered by the
        # user with quotes around them, and the quotes were stripped by the
        # shell. Add the quotes back in so the user's intent is preserved.
        #
        # Otherwise, they are JVM arguments.
        if ($isNameFound) {
            $_ = "\"$_\"" if / /;
            push @$progs, $_;
        } else {
            push @$jvms, $_;
        }
    }

    return($name, $jvms, $progs);
}

sub getNicknames {
    # The nicknames table is a hash table, hash-keys being the nicknames of the
    # Java programs, and the hash-values being an anonymous hash table of
    # classname, string of default JVM args, and string of default program
    # args:
    #
    #       nickname => {
    #           classname => classname-value
    #           jvmargs   => string JVM arguments for this nickname
    #           progargs  => string of program arguments for this nickname
    #       }
    #
    # The configuration file that this structure is generated from has the following format:
    #
    # nickname|fully.specified.classname|space-delimited JVM args|space-delimited program args
    #
    # with "#" as a comment character, a la Perl.  Blank lines are ignored.
    #
    # If there is a line beginning with 'DEFAULT_JVM_ARGS=', the text from the
    # rest of that line is added to every nickname's jvmargs.

    open(my $fh, $pars{conffile})
        or die "Can't open configuration file $pars{conffile}";

    my %nicknames = ();
    my $default_jvm_args = "";
    my $logfile_prefix = "";
    my $logfile="";
    my $logfile_cmdline_property="";
    my $process_logfile_prefix = $_[0];

    while (<$fh>) {
        s/#.*//;        # strip comments
        s/^\s+|\s+$//g; # strip leading/trailing whitespace
        chomp;          # strip EOL \n
        next if /^$/;   # ignore empty lines

        # Capture DEFAULT_JVM_ARGS.
        if (/^DEFAULT_JVM_ARGS=(.*)/) {
            $default_jvm_args = $1;
            next;
        }

        # Capture LOGFILE_PREFIX.
        if (/^LOGFILE_PREFIX=(.*)/) {
            if ($process_logfile_prefix) {
                $logfile_prefix = $1;
                $logfile = File::Spec->catfile($properties{'pipeline.results.dir'}, 'logs', 'cli', $logfile_prefix . '.log');
                $logfile_cmdline_property = ' -Dcli.logfile=' . $logfile;
            }
            next;
        }

        my @fields = split /\|/;
        push @fields, "" while scalar @fields < 4;

        # Load the data into the nicknames structure.

        #nickname -> classname mapping
        $nicknames{$fields[0]}{classname} = $fields[1];

        #nickname -> (defaults) + JVM args mapping
        $nicknames{$fields[0]}{jvmargs} = $default_jvm_args . $logfile_cmdline_property . " " . $fields[2];

        #nickname -> program args mapping
        $nicknames{$fields[0]}{progargs} = $fields[3];

        # Replace any null fields with the empty string to remove
        # concatenation errors later.
        foreach (keys %{$nicknames{$fields[0]}}) {
            $nicknames{$fields[0]}{$_} = ""
                if not defined $nicknames{$fields[0]}{$_};
        }
    }

    close $fh or warn "Error closing configuration file $pars{conffile}";

    return %nicknames;
}

sub displayNicknames {
    # Displays the nicknames in the default (or user-supplied) nicknames
    # file, along with fully-qualified class names of each nickname

    # Get the nicknames out of the nicknames file.
    my %nicknames  = getNicknames(0);
    my $max_key_length = maxKeyLength(\%nicknames);
    my $format = "%" . $max_key_length . "s      %s\n";
    printf($format, "NICKNAME", "CLASSNAME");
    foreach my $key (sort keys %nicknames) {
        printf($format, $key, $nicknames{$key}{classname});
    }
}

sub maxKeyLength {

    my %hash = %{$_[0]};
    my $max_length = -1;
    foreach my $key (keys %hash) {
        my $key_length = length $key;
        if ($key_length > $max_length) {
            $max_length = length $key;
        }
    }
    return $max_length;
}

sub getProperties {
    # Parses a Java properties file and return the properties found. Only
    # lines of the form "var = value" are parsed. Properties escapes are not
    # currently processed.

    defined($pars{properties})
        or die "Properties file not specified with either PIPELINE_CONFIG_PATH or properties option";

    open(my $fh, $pars{properties})
        or die "Can't open properties file $pars{properties}";

    my %properties = ();

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

    close $fh or warn "Error closing properties file $pars{properties}";
    return %properties;
}

sub makeSubstitutions {
    # Substitute property value and environment variable references in a string.

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
            die "Missing Ziggy property '$key'" if (!exists($properties{$key}));
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

=head1 NAME

runjava - A wrapper to run the Ziggy Java tools

=head1 SYNOPSIS

runjava [--help] [--norun] [--[no]verbose] [--conffile=file] [--runjavaHome=dir] [--properties=file] [jvm-args] nickname [program args]

runjava [--help] [--norun] [--[no]verbose] [--conffile=file] [--runjavaHome=dir] [--properties=file] [jvm-args] fully-specified-classname [program args]

=head1 OPTIONS

=over

=item --help

Prints a short help message.

=item --norun

Generate the Java command that would be run, but do not run it.  Useful
when combined with the verbose option to echo out what would be done.
Default is off.

=item --[no]verbose

Print the Java command line as it is being run.  Default is non-verbose.

=item --conffile=file

Specify a non-default nickname file. See the DESCRIPTION section for the
specification of the nickname file format. Default is
runjavaHome/etc/runjava.conf.

=item --runjavaHome=directory

Specify the home directory of the package distribution. Default is
the value of the ziggy.home.dir property.

=item --properties=file

Specify the path to the properties file containing the Ziggy configuration.
Default is $PIPELINE_CONFIG_PATH.

=back

=head1  DESCRIPTION

A wrapper for the Ziggy Java tools, runjava allows the user to run a subset of
the Java tools without having to specify any of the default JVM or program
arguments. Any Ziggy Java tool can be run if the desired JVM and program
arguments are specified. Additional JVM or program arguments can be added to
either type of Ziggy Java tool.

The configuration file specified by the --conffile option or default defines
the tools that are specified for easy use with runjava. Each line in this file
defines a tool's nickname and arguments in the following pipe-delimited
format:

    nickname|fully.specified.classname|string of jvm args|string of program args

Blank lines are ignored and the # character is a comment char, as in Perl.

Additionally, JVM arguments for every nickname in the file can be specified by
including a 'DEFAULT_JVM_ARGS=...', with the elipsis replaced by the desired
JVM arguments string. The text after DEFAULT_JVM_ARGS= will be given as the
first JVM arguments.

A logfile prefix can be specified by including a 'LOGFILE_PREFIX=...', with
the ellipsis replaced by the desired logfile prefix. This will be inserted in
the full Java command as a "-Dcli.logfile=..." clause.

If the user-specified name is a nicknamne in the configuration file, the
defaults specified in the configuration file are used. Otherwise, the user
must specify all necessary parameters, including a fully-specified classname.

Additional arguments can be given on the command line. JVM arguments can be
given before the nickname/class name, and must have a '-' prefix. Program
arguments can be given after the nickname/class name, and do not require a
prefix. The nickname/class name is taken to be the first argument to runjava
without a '-' prefix. The runjava arguments --[no]verbose, --norun,
--conffile, and --runjavaHome are never passed on to the Java call.

=head1 AUTHORS

Kester Allen

=cut
