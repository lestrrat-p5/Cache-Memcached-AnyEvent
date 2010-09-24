package t::CMAETest::Consistency;
use strict;
use AnyEvent;
use Test::More;
use Test::Requires;
use t::Cache::Memcached::AnyEvent::Test;

sub run {
    my ($self, $protocol, $selector) = @_;

    Test::Requires->import( 'Cache::Memcached' );
    SKIP: {
        skip "Can't test with Ketama", 26;

        my $memd_anyevent = test_client(protocol_class => $protocol, selector_class => $selector);
        my $memd = Cache::Memcached->new({
            servers => [ test_servers() ],
            namespace => $memd_anyevent->{namespace},
        });

        my @keys = map { "consistency_$_" } ('a'..'z');
        $memd->flush_all;
        foreach my $key (@keys) {
            $memd->set($key, $key);
        }

        my $cv = AE::cv;
        $memd_anyevent->get_multi(\@keys, sub {
            my $values = shift;
            foreach my $key (@keys) {
                is $values->{$key}, $key, "get_multi returned $key";
            }
            $cv->send();
        });

        $cv->recv;
    }
    done_testing;
}

1;