package t::CMAETest::Dorman;
use strict;
use t::Cache::Memcached::AnyEvent::Test;

sub run {
    my ($pkg, $protocol, $selector) = @_;

    { # populate
        my $cv = AE::cv;
        my $mc = test_client(
            protocol_class => $protocol,
            selector_class => $selector,
            namespace => 'mytest.') or die;
        $cv->begin;
        $mc->set (foo => bar => sub {
            my $rc = shift;
            is $rc, 1, 'Success setting key';
            $cv->end;
        });
        $cv->recv;
    }

    { # run once with regular server list
        my $cv = AE::cv;
        my $mc = test_client(
            protocol_class => $protocol,
            selector_class => $selector,
            namespace => 'mytest.') or die;
        $cv->begin;
        $mc->get (foo => sub {
            my $value = shift;
            is $value, 'bar', 'Success getting key';
            $cv->end;
        });
        $cv->recv;
    }

    { # run another time, but this time shuffle round the server
      # list before creating a client
        my @servers = reverse split /\s*,\s*/, $ENV{PERL_ANYEVENT_MEMCACHED_SERVERS};
        
        local $ENV{PERL_ANYEVENT_MEMCACHED_SERVERS} = join(',', @servers);
        my $cv = AE::cv;
        my $mc = test_client(
            protocol_class => $protocol,
            selector_class => $selector,
            namespace => 'mytest.') or die;
        $cv->begin;
        $mc->get (foo => sub {
            my $value = shift;
            is $value, 'bar', 'Success getting key';
            $cv->end;
        });
        $cv->recv;
    }
    done_testing;
}

1;