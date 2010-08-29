package t::CMAETest::Commands;
use strict;
use AnyEvent::Impl::Perl;
use t::Cache::Memcached::AnyEvent::Test;
use Test::More;
use Test::Exception;

my $key = 'CMAETest.' . int(rand(1000));
my @keys = map { "commands-$_" } (1..10);
my @callbacks = (
    sub {
        my ($memd, $cv) = @_;
        $memd->delete($key, sub { ok(1, 'Delete'); $cv->end });
    },
    sub { my ($memd, $cv) = @_; $memd->flush_all(sub { is($_[0], 1, 'Flush all records'); $cv->end }); },
    sub { my ($memd, $cv) = @_; my $cb = AE::cv {is($_[0]->recv, 1, 'Flush all records'); $cv->end }; $memd->flush_all($cb); },
    sub { my ($memd, $cv) = @_; $memd->get($key, sub { ok(!$_[0], "Get on non-existent value"); $cv->end }) },
    sub { my ($memd, $cv) = @_; my $cb = AE::cv {ok(!$_[0]->recv, "Get on non-existent value"); $cv->end }; $memd->get($key, $cb) },
    sub { my ($memd, $cv) = @_; $memd->add($key, 'v1', sub { ok($_[0], 'Add'); $cv->end }); },
    sub { my ($memd, $cv) = @_; $memd->get($key, sub { is( $_[0], 'v1', 'Fetch'); $cv->end } ); },
    sub { my ($memd, $cv) = @_; $memd->set($key, 'v2', sub { ok($_[0], 'Set'); $cv->end }); },
    sub { my ($memd, $cv) = @_; $memd->get($key, sub { is( $_[0], 'v2', 'Fetch'); $cv->end }); },
    sub { my ($memd, $cv) = @_; $memd->replace($key, 'v3', sub { ok($_[0], 'Replace'); $cv->end }); },
    sub { my ($memd, $cv) = @_; $memd->get($key, sub { is( $_[0], 'v3', 'Fetch'); $cv->end }); },
    sub { my ($memd, $cv) = @_; $memd->replace($key, 0, sub { ok( $_[0], 'replace with numeric'); $cv->end }); },
    sub { my ($memd, $cv) = @_; $memd->get($key, sub { is($_[0], 0, 'Replace turned out to be 0'); $cv->end }); },
    sub { my ($memd, $cv) = @_; $memd->incr($key, sub { ok($_[0], 'Incr'); $cv->end }); },
    sub { my ($memd, $cv) = @_; $memd->get($key, sub { is($_[0], 1, 'Fetch'); $cv->end }); },
    sub { my ($memd, $cv) = @_; $memd->incr($key, 5, sub { ok($_[0], 'Incr 5'); $cv->end }); },
    sub { my ($memd, $cv) = @_; $memd->incr('no-such-key', 5, sub { ok(!$_[0], 'Incr no_such_key'); $cv->end }); },
    sub { my ($memd, $cv) = @_; $memd->get($key, sub { is($_[0], 6, 'Fetch'); $cv->end }); },
    sub { my ($memd, $cv) = @_; $memd->decr($key, sub { ok($_[0], 'Decr'); $cv->end }); },
    sub { my ($memd, $cv) = @_; $memd->get($key, sub { is($_[0], 5, 'Fetch'); $cv->end }); },
    sub { my ($memd, $cv) = @_; $memd->decr($key, sub { is($_[0], 4, 'Decr'); $cv->end }); },
    sub { my ($memd, $cv) = @_; $memd->get($key, sub { is($_[0], 4, 'Fetch'); $cv->end }); },
    sub { my ($memd, $cv) = @_; $memd->decr($key, 100, sub { is($_[0], 0, 'Decr below zero'); $cv->end }); },
    sub { my ($memd, $cv) = @_; $memd->decr($key, 100, sub { is($_[0], 0, 'Decr below zero returns true value'); $cv->end }); },
    sub { my ($memd, $cv) = @_; $memd->get($key, sub { is($_[0], 0, 'Fetch'); $cv->end }); },
    sub { my ($memd, $cv) = @_; $memd->get_multi([], sub { ok($_[0], 'get_multi() with empty list'); $cv->end }); },
    sub {
        my ($memd, $cv) = @_;
        my $xcv = AE::cv { $cv->end };
        foreach my $key (@keys) {
            $xcv->begin;
            $memd->set( $key, $key, sub { ok($_[0], "set $key"); $xcv->end });
        }
    },
    sub {
        my ($memd, $cv) = @_;
        $memd->get_multi(\@keys, sub {
            my $h = shift;
            foreach my $key (@keys) {
                is($h->{$key}, $key, "Key $key match");
            }
            $cv->end;
        });
    },
    sub {
        my ($memd, $cv) = @_;
        $memd->set( $key,  "abc", sub { $cv->end } );
    },
    sub {
        my ($memd, $cv) = @_;
        $memd->append( $key, 'def', sub { ok ($_[0], "append $key"); $cv->end } );
    },
    sub {
        my ($memd, $cv) = @_;
        $memd->get( $key, sub { is ($_[0], 'abcdef', "append result ok for $key"); $cv->end } );
    },
    sub {
        my ($memd, $cv) = @_;
        $memd->prepend( $key, '123', sub { ok ($_[0], "prepend $key"); $cv->end } );
    },
    sub {
        my ($memd, $cv) = @_;
        $memd->get( $key, sub { is ($_[0], '123abcdef', "prepend result ok for $key"); $cv->end } );
    },
    sub { my ($memd, $cv) = @_; $memd->flush_all(sub { is($_[0], 1, 'Flush all records'); $cv->end }); },
    sub { my ($memd, $cv) = @_; $memd->get($key, sub { ok(!$_[0], "Get on existing value fails after flush_all"); $cv->end }) },
);

sub run {
    my ($pkg, $protocol, $selector) = @_;

    lives_ok {
        my $cv = AE::cv;
        my $memd = test_client(protocol_class => $protocol, selector_class => $selector);

        isa_ok $memd->selector, "Cache::Memcached::AnyEvent::Selector::$selector";
        isa_ok $memd->protocol, "Cache::Memcached::AnyEvent::Protocol::$protocol";

        $cv->begin;
        $memd->version( sub {
            while ( my($host_port, $version) = each %{$_[0]} ) {
                note("[$protocol/$selector] using memcached $version on $host_port");
            }
            $cv->end;
        } );

        $cv->recv;

        $cv = AE::cv;
        foreach my $code (@callbacks) {
            $cv->begin;
            eval {
                $code->($memd, $cv);
            };
            if ($@) {
                ok(0, "an error occurred: $@");
                $cv->end;
            }
        }
        $cv->recv;
    } "Command tests ran fine";
    done_testing;
}

1;