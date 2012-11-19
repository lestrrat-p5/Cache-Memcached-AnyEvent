use strict;
my @devmods = qw(
    Module::Install::AuthorTests
    Module::Install::ReadmeFromPod
    Module::Install::Repository
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

