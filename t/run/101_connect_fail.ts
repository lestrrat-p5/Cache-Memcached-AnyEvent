use strict;
use lib "t/lib";
use Cache::Memcached::AnyEvent::Test;

my $memd = test_client() or exit;
plan tests => 51;

my $bogus_server = 'you.should.not.be.able.to.connect.to.me:11211';
push @{$memd->{servers}}, $bogus_server;

my $key_base = "BigF*ckingTruckHitMe";
my $value = join('.', time(), $$, {}, rand());

my $cv = AE::cv;

local $SIG{__WARN__} = sub {
    like( $_[0], qr/^failed to connect to $bogus_server/);
};

foreach my $i (1..50) {
    my $key = $key_base . $i;
    $cv->begin;
    $memd->set($key, $value, sub {});
    $memd->get($key, sub { is ($_[0], $value, "values match"); $cv->end });
}

$cv->recv;