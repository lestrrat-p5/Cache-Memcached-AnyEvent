package t::run::102_cv_t;

use strict;
use lib "t/lib";
use AnyEvent::Impl::Perl;
use Cache::Memcached::AnyEvent::Test;

my $memd = test_client() or exit;
plan tests => 3;

# count should be >= 4.
use constant count => 10;

my $key = 'CMAETest.' . int(rand(1000));
my @keys = map { "commands-$_" } (1..count);

my $cv = AE::cv { ok(1, "delete 'returns' ok") };
$memd->delete($key, $cv);

$cv->recv;

$cv = AE::cv { ok($_[0]->recv, "set ok") };
$memd->set($key, "foo", $cv);
$cv->recv;

use Data::Dumper;
$cv = AE::cv { is($_[0]->recv, "foo", "get ok") };
$memd->get($key, $cv);
$cv->recv;
