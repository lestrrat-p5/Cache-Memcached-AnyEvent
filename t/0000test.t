#!env perl

use strict;
use warnings;
use AnyEvent;
use Cache::Memcached::AnyEvent;
use Test::More tests => 41;
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

my $cv = AE::cv;
$cv->begin;

{
    my $mc = Cache::Memcached::AnyEvent->new ({
                                               servers => [split /,/, $ENV{PERL_ANYEVENT_MEMCACHED_SERVERS}],
                                               namespace => 'mytest.'
                                              });
    $cv->begin;
    $mc->set (foo => bar => sub {
                  my $rc = shift;
                  is $rc, 1, 'Success setting key';
                  $cv->end;
              });
}

$cv->end;
$cv->recv;

$cv = AE::cv;
$cv->begin;
for (1..40) {
    my $mc = Cache::Memcached::AnyEvent->new ({
                                               servers => [split /,/, $ENV{PERL_ANYEVENT_MEMCACHED_SERVERS}],
                                               namespace => 'mytest.'
                                              });
    $cv->begin;
    $mc->get (foo => sub {
                  my $value = shift;
                  is $value, 'bar', 'Success getting key';
                  $cv->end;
              });
}

$cv->end;
$cv->recv;
