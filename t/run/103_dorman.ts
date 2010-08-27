package t::run::103_dorman;
use strict;
use t::Cache::Memcached::AnyEvent::Test;

my $cv = AE::cv;
{ # populate
    my $mc = test_client(namespace => 'mytest.') or die;
    $cv->begin;
    $mc->set (foo => bar => sub {
        my $rc = shift;
        is $rc, 1, 'Success setting key';
        $cv->end;
    });
}

$cv->recv;

$cv = AE::cv;
for (1..40) {
    my $mc = test_client(namespace => 'mytest.') or die;
    $cv->begin;
    $mc->get (foo => sub {
        my $value = shift;
        is $value, 'bar', 'Success getting key';
        $cv->end;
    });
}

$cv->recv;

done_testing;
