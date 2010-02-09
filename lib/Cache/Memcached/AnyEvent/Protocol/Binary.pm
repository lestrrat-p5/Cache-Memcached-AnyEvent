package Cache::Memcached::AnyEvent::Protocol::Binary;
use strict;
use base 'Cache::Memcached::AnyEvent::Protocol';
use Carp qw(confess);
use constant HEADER_SIZE => 24;
use constant HAS_64BIT => do {
    no strict;
    require Config;
    $Config{use64bitint} || $Config{use64bitall};
};

#   General format of a packet:
#
#     Byte/     0       |       1       |       2       |       3       |
#        /              |               |               |               |
#       |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
#       +---------------+---------------+---------------+---------------+
#      0/ HEADER                                                        /
#       /                                                               /
#       /                                                               /
#       /                                                               /
#       +---------------+---------------+---------------+---------------+
#     16/ COMMAND-SPECIFIC EXTRAS (as needed)                           /
#      +/  (note length in th extras length header field)               /
#       +---------------+---------------+---------------+---------------+
#      m/ Key (as needed)                                               /
#      +/  (note length in key length header field)                     /
#       +---------------+---------------+---------------+---------------+
#      n/ Value (as needed)                                             /
#      +/  (note length is total body length header field, minus        /
#      +/   sum of the extras and key length body fields)               /
#       +---------------+---------------+---------------+---------------+
#      Total 16 bytes
#
#   Request header:
#
#     Byte/     0       |       1       |       2       |       3       |
#        /              |               |               |               |
#       |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
#       +---------------+---------------+---------------+---------------+
#      0| Magic         | Opcode        | Key length                    |
#       +---------------+---------------+---------------+---------------+
#      4| Extras length | Data type     | Reserved                      |
#       +---------------+---------------+---------------+---------------+
#      8| Total body length                                             |
#       +---------------+---------------+---------------+---------------+
#     12| Opaque                                                        |
#       +---------------+---------------+---------------+---------------+
#     16| CAS                                                           |
#       |                                                               |
#       +---------------+---------------+---------------+---------------+
#       Total 24 bytes
#
#   Response header:
#
#     Byte/     0       |       1       |       2       |       3       |
#        /              |               |               |               |
#       |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
#       +---------------+---------------+---------------+---------------+
#      0| Magic         | Opcode        | Status                        |
#       +---------------+---------------+---------------+---------------+
#      4| Extras length | Data type     | Reserved                      |
#       +---------------+---------------+---------------+---------------+
#      8| Total body length                                             |
#       +---------------+---------------+---------------+---------------+
#     12| Opaque                                                        |
#       +---------------+---------------+---------------+---------------+
#     16| CAS                                                           |
#       |                                                               |
#       +---------------+---------------+---------------+---------------+
#       Total 24 bytes
#
#   Header fields:
#   Magic               Magic number.
#   Opcode              Command code.
#   Key length          Length in bytes of the text key that follows the
#                       command extras.
#   Status              Status of the response (non-zero on error).
#   Extras length       Length in bytes of the command extras.
#   Data type           Reserved for future use (Sean is using this
#                       soon).
#   Reserved            Really reserved for future use (up for grabs).
#   Total body length   Length in bytes of extra + key + value.
#   Opaque              Will be copied back to you in the response.
#   CAS                 Data version check.

# Constants
use constant +{
#    Magic numbers
    REQ_MAGIC       => 0x80,
    RES_MAGIC       => 0x81,

#    Status Codes
#    0x0000  No error
#    0x0001  Key not found
#    0x0002  Key exists
#    0x0003  Value too large
#    0x0004  Invalid arguments
#    0x0005  Item not stored
#    0x0006  Incr/Decr on non-numeric value.
    ST_SUCCESS      => 0x0000,
    ST_NOT_FOUND    => 0x0001,
    ST_EXISTS       => 0x0002,
    ST_TOO_LARGE    => 0x0003,
    ST_INVALID      => 0x0004,
    ST_NOT_STORED   => 0x0005,
    ST_NON_NUMERIC  => 0x0006,

#    Opcodes
    MEMD_GET        => 0x00,
    MEMD_SET        => 0x01,
    MEMD_ADD        => 0x02,
    MEMD_REPLACE    => 0x03,
    MEMD_DELETE     => 0x04,
    MEMD_INCREMENT  => 0x05,
    MEMD_DECREMENT  => 0x06,
    MEMD_QUIT       => 0x07,
    MEMD_FLUSH      => 0x08,
    MEMD_GETQ       => 0x09,
    MEMD_NOOP       => 0x0A,
    MEMD_VERSION    => 0x0B,
    MEMD_GETK       => 0x0C,
    MEMD_GETKQ      => 0x0D,
    MEMD_APPEND     => 0x0E,
    MEMD_PREPEND    => 0x0F,
    MEMD_STAT       => 0x10,
    MEMD_SETQ       => 0x11,
    MEMD_ADDQ       => 0x12,
    MEMD_REPLACEQ   => 0x13,
    MEMD_DELETEQ    => 0x14,
    MEMD_INCREMENTQ => 0x15,
    MEMD_DECREMENTQ => 0x16,
    MEMD_QUITQ      => 0x17,
    MEMD_FLUSHQ     => 0x18,
    MEMD_APPENDQ    => 0x19,
    MEMD_PREPENDQ   => 0x1A,
    RAW_BYTES       => 0x00,
};

my $OPAQUE;
BEGIN {
    $OPAQUE = 0xffffffff;
}

# binary protocol read type
AnyEvent::Handle::register_read_type memcached_bin => sub {
    my ($self, $cb) = @_;

    my %state = ( waiting_header => 1 );
    sub {
        if ($state{waiting_header}) {
            bytes::length ( $_[0]{rbuf} || '') >= HEADER_SIZE or return;
            my $header = bytes::substr $_[0]{rbuf}, 0, HEADER_SIZE, "";
            my ($magic, $opcode, $key_length, $extra_length, $status, $data_type, $total_body_length, $opaque, $cas) = _decode_header($header);
            $state{magic} = $magic;
            $state{opcode} = $opcode;
            $state{key_length} = $key_length;
            $state{extra_length} = $extra_length;
            $state{status} = $status;
            $state{data_type} = $data_type;
            $state{total_body_length} = $total_body_length;
            $state{opaque} = $opaque;
            $state{cas} = $cas;

            delete $state{waiting_header};
        }

        if ($state{total_body_length}) {
            bytes::length $_[0]{rbuf} >= $state{total_body_length} or return;

            $state{extra} = bytes::substr $_[0]{rbuf}, 0, $state{extra_length}, '';
            $state{key} = bytes::substr $_[0]{rbuf}, 0, $state{key_length}, '';


            my $value_len = $state{total_body_length} - ($state{key_length} + $state{extra_length});
            $state{value} = bytes::substr $_[0]{rbuf}, 0, $value_len, '';
        }

        $cb->( \%state );
        undef %state;
        1;
    }
};

sub prepare_handle {
    my ($self, $fh) = @_;
    binmode($fh);
}

AnyEvent::Handle::register_write_type memcached_bin => sub {
    my ($self, @args) = @_;
    return _encode_request(@args);
};

sub _encode_request {
    my ( $opcode, $key, $extras, $body, $cas, $data_type, $reserved ) = @_;

    use bytes;

    my $key_length = defined $key ? bytes::length($key) : 0;
    # first 4 bytes (long)
    my $i1 = 0;
    $i1 ^= REQ_MAGIC << 24;
    $i1 ^= $opcode << 16;
    $i1 ^= $key_length;

    # second 4 bytes
    my $extra_length = defined $extras ? bytes::length($extras) : 0;
    my $i2 = 0;
    $i2 ^= $extra_length << 24;
    # $data_type and $reserved are not used currently

    # third 4 bytes
    my $body_length  = defined $body ? bytes::length($body) : 0;
    my $i3 = $body_length + $key_length + $extra_length;

    # this is the opaque value, which will be returned with the response
    my $i4 = $OPAQUE;
    if ($OPAQUE == 0xffffffff) {
        $OPAQUE = 0;
    } else {
        $OPAQUE++;
    }

    # CAS is 64 bit, which is troublesome on 32 bit architectures.
    # we will NOT allow 64 bit CAS on 32 bit machines for now.
    # better handling by binary-adept people are welcome
    $cas ||= 0;
    my ($i5, $i6);
    if (HAS_64BIT) {
        no warnings;
        $i5 = 0xffffffff00000000 & $cas;
        $i6 = 0x00000000ffffffff & $cas;
    } else {
        $i5 = 0x00000000;
        $i6 = $cas;
    }

    my $message = pack( 'N6', $i1, $i2, $i3, $i4, $i5, $i6 );
    if (bytes::length($message) > HEADER_SIZE) {
        confess "header size assertion failed";
    }

    if ($extra_length) {
        $message .= $extras;
    }
    if ($key_length) {
        $message .= pack('a*', $key);
    }
    if ($body_length) {
        $message .= pack('a*', $body);
    }

    return $message;
};

use constant _noop => _encode_request(MEMD_NOOP, undef, undef, undef, undef, undef, undef);

sub _decode_header {
    my $header = shift;

    my ($i1, $i2, $i3, $i4, $i5, $i6) = unpack('N6', $header);
    my $magic = $i1 >> 24;
    my $opcode = ($i1 & 0x00ff0000) >> 16;
    my $key_length = $i1 & 0x0000ffff;
    my $extra_length = ($i2 & 0xff000000) >> 24;
    my $data_type = ($i2 & 0x00ff0000) >> 8;
    my $status = $i2 & 0x0000ffff;
    my $total_body_length = $i3;
    my $opaque = $i4;

    my $cas;
    if (HAS_64BIT) {
        $cas = $i5 << 32;
        $cas += $i6;
    } else {
        warn "overflow on CAS" if ($i5 || 0) != 0;
        $cas = $i6;
    }

    return ($magic, $opcode, $key_length, $extra_length, $status, $data_type, $total_body_length, $opaque, $cas);
}

sub _status_str {
    my $status = shift;
    my %strings = (
        ST_SUCCESS() => "Success",
        ST_NOT_FOUND() => "Not found",
        ST_EXISTS() => "Exists",
        ST_TOO_LARGE() => "Too Large",
        ST_INVALID() => "Invalid Arguments",
        ST_NOT_STORED() => "Not Stored",
        ST_NON_NUMERIC() => "Incr/Decr on non-numeric variables"
    );
    return $strings{$status};
}

# Generate setters
{
    my %bincmd = (
        add => MEMD_ADD(),
        replace => MEMD_REPLACE(),
        set => MEMD_SET(),
    );
    my $generator = sub {
        my $cmd = shift;

        my $bincmd = $bincmd{$cmd};
        sub {
            my ($guard, $self, $memcached, $key, $value, $exptime, $noreply, $cb) = @_;
            my $fq_key = $self->prepare_key( $key );
            my $handle = $self->get_handle_for( $fq_key );

            my ($write_data, $write_len, $flags, $expires) =
                $self->prepare_value( $cmd, $value, $exptime || 0);

            my $extras = pack('N2', $flags, $expires);

            $handle->push_write( memcached_bin => $bincmd, $fq_key, $extras, $write_data );
            $handle->push_read( memcached_bin => sub {
                undef $guard;
                $cb->($_[0]->{status} == 0, $_[0]->{value}, $_[0]);
            });
        }
    };

    sub _build_add_cb {
        my $self = shift;
        return $generator->("add");
    }

    sub _build_replace_cb {
        my $self = shift;
        return $generator->("replace");
    }

    sub _build_set_cb {
        my $self = shift;
        return $generator->("set");
    }
}

sub _build_delete_cb {
    return sub {
        my ($guard, $self, $memcached, $key, $noreply, $cb) = @_;

        my $fq_key = $self->prepare_key($key);
        my $handle = $self->get_handle_for($fq_key);

        $handle->push_write( memcached_bin => MEMD_DELETE, $fq_key );
        $handle->push_read( memcached_bin => sub {
            undef $guard;
            $cb->(@_);
        } );
    }
}

sub _build_get_multi_cb {
    return sub {
        my ($guard, $self, $memcached, $type, $keys, $cb, $cb_caller) = @_;

        # organize the keys by handle
        my %handle2keys;
            
        foreach my $key (@$keys) {
            my $fq_key = $self->prepare_key( $key );
            my $handle = $self->get_handle_for( $fq_key );
            my $list = $handle2keys{ $handle };
            if (! $list) {
                $handle2keys{$handle} = [ $handle, $fq_key ];
            } else {
                push @$list, $fq_key;
            }
        }

        my $cv = $type eq 'single' ?
            AE::cv {
                undef $guard;
                my ($rv, $msg) = $_[0]->recv;
                if ($rv) {
                    ($rv) = values %$rv;
                }
                $cb->($rv, $msg);
            } :
            AE::cv {
                undef $guard;
                $cb->( $_[0]->recv );
            }
        ;

        my %result;
        $cv->begin( sub { $_[0]->send(\%result) } );
        foreach my $list (values %handle2keys) {
            my ($handle, @keys) = @$list;
            foreach my $key ( @keys ) {
                $handle->push_write(memcached_bin => MEMD_GETK, $key);
                $cv->begin;
                $handle->push_read(memcached_bin => sub {
                    my $msg = shift;

                    my ($flags, $exptime) = unpack('N2', $msg->{extra});
                    if (exists $msg->{key} && exists $msg->{value}) {
                        my $key = $self->decode_key($key);
                        my $value = $self->decode_value( $flags, $msg->{value} );
                        $result{ $key } = $value;
                    }
                    $cv->end;
                });

                $handle->push_write( _noop() );
                $handle->push_read( memcached_bin => sub {} );
            }
        }
        $cv->end;
    };
}

{
    my $generator = sub {
        my ($opcode) = @_;
        return sub {
            my ($guard, $self, $memcached, $key, $value, $initial, $cb) = @_;
    
            $value ||= 1;
            my $expires = defined $initial ? 0 : 0xffffffff;
            $initial ||= 0;
            my $fq_key = $self->prepare_key( $key );
            my $handle = $self->get_handle_for($fq_key);
            my $extras;
            if (HAS_64BIT) {
                $extras = pack('Q2L', $value, $initial, $expires );
            } else {
                $extras = pack('N5', 0, $value, 0, $initial, $expires );
            }
    
            $handle->push_write(memcached_bin => 
                $opcode, $fq_key, $extras, undef, undef, undef, undef);
            $handle->push_read(memcached_bin => sub {
                undef $guard;
                my $value;
                if (HAS_64BIT) {
                    $value = unpack('Q', $_[0]->{value});
                } else {
                    (undef, $value) = unpack('N2', $_[0]->{value});
                }

                $cb->($_[0]->{status} == 0 ? $value : undef, $_[0]);
            } );
        }
    };

    sub _build_incr_cb {
        return $generator->(MEMD_INCREMENT);
    }

    sub _build_decr_cb {
        return $generator->(MEMD_DECREMENT);
    }
};

sub _build_version_cb {
    return sub {
        my ($guard, $self, $memcached, $cb) = @_;

        my %ret;
        my $cv = AE::cv { $cb->( \%ret ); undef %ret };
        while (my ($host_port, $handle) = each %{ $memcached->{_server_handles} }) {
            $handle->push_write(memcached_bin => MEMD_VERSION);
            $cv->begin;
            $handle->push_read(memcached_bin => sub {
                my $msg = shift;
                undef $guard;
                my $value = unpack('a', $msg->{value});

                $ret{ $host_port } = $value;
                $cv->end;
            });
        }
    }
}
        

1;