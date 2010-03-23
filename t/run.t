use strict;
use Test::More;
use Test::Memcached;


my $memd;
if ( ! $ENV{PERL_ANYEVENT_MEMCACHED_SERVERS}) {
    $memd = Test::Memcached->new();
    if ($memd) {
        $memd->start();
        $ENV{PERL_ANYEVENT_MEMCACHED_SERVERS} = '127.0.0.1:' . $memd->option('tcp_port');
    }
}

my @files = <t/run/*.ts>;

plan tests => scalar @files;

foreach my $file (@files) {
    subtest "run $file" => sub { do $file };
}