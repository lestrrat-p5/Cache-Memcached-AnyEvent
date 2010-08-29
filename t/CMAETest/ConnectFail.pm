package t::CMAETest::ConnectFail;
use strict;
use Test::More;
use t::Cache::Memcached::AnyEvent::Test;

sub run {
    my ($pkg, $protocol, $selector) = @_;
    my $memd = test_client(protocol_class => $protocol, selector_class => $selector);

    my $bogus_server = 'you.should.not.be.able.to.connect.to.me:11211';
    push @{$memd->{servers}}, $bogus_server;

    my $key_base = "BigF*ckingTruckHitMe";
    my $value = join('.', time(), $$, {}, rand());

    my $cv = AE::cv;

    my $warn_called = 0;
    local $SIG{__WARN__} = sub {
        like( $_[0], qr/^failed to connect to $bogus_server/);
        $warn_called++;
    };

    foreach my $i (1..50) {
        my $key = $key_base . $i;
        $cv->begin;
        $memd->set($key, $value, sub {});
        $memd->get($key, sub { is ($_[0], $value, "values match"); $cv->end });
    }

    $cv->recv;

    ok $warn_called, "warn properly called";
    done_testing;
}

1;