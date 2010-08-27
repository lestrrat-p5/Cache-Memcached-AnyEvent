#!env perl

use strict;
use warnings;
use AnyEvent;
use Cache::Memcached::AnyEvent;
use POSIX qw{sys_wait_h};
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

#my @ports = (11217..11230);
#my @ports = (11217..11217);

#my @servers = map {"127.0.0.1:$_"} @ports;

#my @memcaches = map {memcached ($_)} @ports;

sleep 1;

#warn join "\n", @memcaches;

my $cv = AE::cv;
$cv->begin;

{
#    warn "Starting tests\n";

    my $mc = Cache::Memcached::AnyEvent->new ({
                                               servers => [split /,/, $ENV{PERL_ANYEVENT_MEMCACHED_SERVERS}],
                                               namespace => 'mytest.',
                                              });
    $cv->begin;
#    warn "Setting key\n";
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
                                               namespace => 'mytest.',
                                              });
    $cv->begin;
#    warn "Retrieving key\n";
    $mc->get (foo => sub {
                  my $value = shift;
                  is $value, 'bar', 'Success getting key';
                  $cv->end;
              });
}

$cv->end;
$cv->recv;

#warn "Done with tests\n";


#warn "Terminating memcached\n";

#map {terminate ($_)} @memcaches;

sub memcached {
    my $port = shift;

    #warn "Starting memcached on port $port\n";

    # Fork a new process
    my $pid = fork;

    # Fork failed, catastrophe!
    die "Couldn't fork\n" unless defined $pid;

    # Perhaps we will get more sophisticated, but for the moment, fork
    # worked, we're happy
    return $pid if ($pid);

    # We're in the child now
    open STDIN, '/dev/null' or die ("Couldn't redirect STDIN");
    open STDOUT, ">t/$port.log" or die ("Couldn't redirect STDOUT");
    open STDERR, '>&STDOUT' or die ("Couldn't redirect STDERR");

    exec "/usr/bin/memcached", "-l", "127.0.0.1", "-p", $port, '-vv';

    die "Failed to exec: $!";
}

sub terminate {
    my ($pid) = @_;

    my $result = 0;

    for my $sig (qw{TERM HUP QUIT INT KILL}) {
        kill ($sig, $pid);
        if (waitpid ($pid, WNOHANG) == $pid) {
            $result = 1;
            last;
        }
        sleep 1;
    }

    return $result;
}
