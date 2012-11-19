use strict;

# Cache::Memcached is not required, but since Travis CI runs author
# tests anyway, we might as well add them so all tests are run
my @devmods = qw(
    Module::Install::AuthorTests
    Module::Install::ReadmeFromPod
    Module::Install::Repository

    Cache::Memcached
);

foreach my $module (@devmods) {
    system( "cpanm", "-n", "-v", $module ) == 0 ||
        die "Failed to install $module!";
}

system("cpanm", "--installdeps", ".") == 0 ||
    die "Failed to run cpanm --installdeps";

system($^X, "Makefile.PL") == 0 ||
    die "Failed to run perl Makefile.PL";

system("make", "test") == 0 ||
    die "Failed to run make test";

