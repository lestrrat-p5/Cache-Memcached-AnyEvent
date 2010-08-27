#!env perl

use strict;
use warnings;
use AnyEvent;
use Cache::Memcached::AnyEvent;
use POSIX qw{sys_wait_h};
use Test::More tests => 41;

my @ports = (11217..11230);
#my @ports = (11217..11217);

my @servers = map {"127.0.0.1:$_"} @ports;

my @memcaches = map {memcached ($_)} @ports;

sleep 1;

#warn join "\n", @memcaches;

my $cv = AE::cv;
$cv->begin;

{
#    warn "Starting tests\n";

    my $mc = Cache::Memcached::AnyEvent->new ({
                                               servers => \@servers,
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
                                               servers => \@servers,
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

map {terminate ($_)} @memcaches;

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
