use strict;
use Test::More;
use Test::Memcached;

my @memd;
if ( ! $ENV{PERL_ANYEVENT_MEMCACHED_SERVERS}) {
    my $port;
    for (1..5) {
        my $memd = Test::Memcached->new(base_dir => 't', options => { verbose => 1 });
            
        if (! $memd) {
            plan skip_all => "Failed to start memcached server";
        }
        if ($port) {
            $memd->start( tcp_port => $port );
        } else {
            $memd->start();
        }

        if ($port) {
            $port++;
        } else {
            $port = $memd->option('tcp_port') + 1;
        }

        # give it a second for the server to start
        push @memd, $memd;
    }

    $ENV{PERL_ANYEVENT_MEMCACHED_SERVERS} = join(',', 
        map { sprintf('127.0.0.1:%d', $_->option('tcp_port')) } @memd
    );
}

my @files = <t/run/*.ts>;

foreach my $file (@files) {
    subtest "run $file" => sub { do $file };
}

done_testing();