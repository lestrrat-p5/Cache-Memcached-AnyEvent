use strict;
use blib;
use Cache::Memcached;
use Cache::Memcached::Fast;
use Cache::Memcached::AnyEvent;
use Data::Dumper;
use Benchmark qw(cmpthese);

print <<EOM;

Cache::Memcached::AnyEvent Benchmark
------------------------------------

1) You should always run this benchmark with MULTIPLE memcached servers.
   Event-driven tools always work best when there are multiple I/O channels
   to multiplex with.

2) Your servers should be specified in MEMCACHED_SERVERS environment variable.
   Multiple server names should be separated by comma. If the variable is not
   set, 127.0.0.1:11211, 127.0.0.1:11212, 127.0.0.1:11213 are assumed.

EOM

my @guards;
my @servers;
if ($ENV{MEMCACHED_SERVERS}) {
    @servers = split /,/, $ENV{MEMCACHED_SERVERS};
} else {
    require Test::Memcached;
    for (1..5) {
        my $memd = Test::Memcached->new();
        $memd->start();
        push @guards, $memd;
        push @servers, join(':', '127.0.0.1', $memd->option('tcp_port') );
    }
}

my %args = (
    servers => \@servers,
    namespace => join('.', time(), $$, rand(), '')
);
my $memd = Cache::Memcached->new(\%args);
my $memd_fast = Cache::Memcached::Fast->new(\%args);
my $memd_anyevent = Cache::Memcached::AnyEvent->new(\%args);
my $memd_anyevent_ketama = Cache::Memcached::AnyEvent->new({ %args,
    selector_class => 'Ketama'
});
my $memd_anyevent_bin = Cache::Memcached::AnyEvent->new({ %args,
    protocol_class => 'Binary',
});
# my $anyevent_memcached = AnyEvent::Memcached->new(%args);

print <<EOM;

Servers: @servers
Cache::Memcached: $Cache::Memcached::VERSION
Cache::Memcached::Fast: $Cache::Memcached::Fast::VERSION
Cache::Memcached::AnyEvent: $Cache::Memcached::AnyEvent::VERSION

EOM

my @keys = ('a'..'z');
$memd->set($_ => $_ x 2) for (@keys);
use Data::Dumper;
cmpthese(-1 => {
    memd          => sub {
        for (1..100) {
            my $values = $memd->get_multi(@keys);
        }
    },
    memd_fast     => sub {
        for (1..100) {
            my $values = $memd->get_multi(@keys);
        }
    },
    memd_anyevent => sub {
        my $cv = AE::cv;
        for (1..100) {
            $cv->begin;
            $memd_anyevent->get_multi(\@keys, sub {
                my $values = shift;
                $cv->end;
            } );
        }
        $cv->recv;
    },
    memd_anyevent_ketama => sub {
        my $cv = AE::cv;
        for (1..100) {
            $cv->begin;
            $memd_anyevent_ketama->get_multi(\@keys, sub {
                my $values = shift;
                $cv->end;
            } );
        }
        $cv->recv;
    },
    memd_anyevent_bin => sub {
        my $cv = AE::cv;
        for (1..100) {
            $cv->begin;
            $memd_anyevent_bin->get_multi(\@keys, sub {
                my $values = shift;
                $cv->end;
            } );
        }
        $cv->recv;
    },
});

