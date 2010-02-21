use inc::Module::Install;

name 'Cache-Memcached-AnyEvent';
all_from 'lib/Cache/Memcached/AnyEvent.pm';
requires 'AnyEvent';
requires 'AnyEvent::Socket';
requires 'AnyEvent::Handle';

requires 'String::CRC32'; # sorry, required

test_requires 'Test::Memcached';

recommends 'Compress::Zlib';

readme_from 'lib/Cache/Memcached/AnyEvent.pm';
auto_set_repository;

WriteAll;

# Fixup Makefile cause we like it!
if (-f 'Makefile') {
    open (my $fh, '<', 'Makefile') or die "Could not open Makefile: $!";
    my $makefile = do { local $/; <$fh> };
    close $fh or die $!;

    $makefile =~ s/"-e" "(test_harness\(\$\(TEST_VERBOSE\), )/"-I\$(INST_LIB)" "-I\$(INST_ARCHLIB)" "-It\/lib" "-MTest::Memcached" "-e" "\\\$\$SIG{INT} = sub { CORE::exit }; my \\\$\$memd; if ( ! \\\$\$ENV{PERL_ANYEVENT_MEMCACHED_SERVERS}) { \\\$\$memd = Test::Memcached->new(); if (\\\$\$memd) { \\\$\$memd->start(); \\\$\$ENV{PERL_ANYEVENT_MEMCACHED_SERVERS} = '127.0.0.1:' . \\\$\$memd->option('tcp_port'); } } $1/;

    open (my $fh, '>', 'Makefile') or die "Could not open Makefile: $!";
    print $fh $makefile;
    close $fh or die $!;
}
