use strict;
use AnyEvent::Impl::Perl;
use t::Cache::Memcached::AnyEvent::Test;

my $memd = test_client() or
    fail("Could not create memcached client instance");

my $key = 'CMAETest.' . int(rand(1000));
my @keys = map { "commands-$_" } (1..4);

my $cv = AE::cv { ok(1, "delete 'returns' ok") };
$memd->delete($key, $cv);

$cv->recv;

$cv = AE::cv { ok($_[0]->recv, "set ok") };
$memd->set($key, "foo", $cv);
$cv->recv;

$cv = AE::cv { is($_[0]->recv, "foo", "get ok") };
$memd->get($key, $cv);
$cv->recv;

done_testing();
