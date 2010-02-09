package Cache::Memcached::AnyEvent::Protocol;
use strict;

sub new {
    my $class = shift;
    my $self  = bless {@_}, $class;

    foreach my $cb qw(add_cb decr_cb delete_cb incr_cb get_cb get_multi_cb replace_cb set_cb stats_cb version_cb) {
        my $method = "_build_$cb";
        $self->{$cb} ||= $self->$method;
    }
    return $self;
}

sub _build_add_cb {}
sub _build_decr_cb {}
sub _build_delete_cb {}
sub _build_incr_cb {}
sub _build_get_cb {}
sub _build_get_multi_cb {}
sub _build_replace_cb {}
sub _build_set_cb {}
sub _build_stats_cb {}
sub _build_version_cb {}

sub prepare_handle {}

1;

__END__

=head1 NAME

Cache::Memcached::AnyEvent::Protocol - Base Class For Memcached Protocol

=head1 SYNOPSIS

    package NewProtocol;
    use strict;
    use base 'Cache::Memcached::AnyEvent::Protocol';

=cut