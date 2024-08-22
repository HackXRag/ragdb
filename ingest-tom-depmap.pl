use strict;
use DBI;
use File::Basename;
use Nanoid;

my $dbh = DBI->connect("dbi:mysql:RagTest;host=arborvitae.cels.anl.gov", 'rag');

$dbh or die;


@ARGV == 2 or die "Usage: $0 pdf-dir txt-dir\n";

my $pdf_dir = shift;
my $txt_dir = shift;

my $tom_parser = 1;

my $sth_pdf = $dbh->prepare(qq(INSERT INTO SourceDocument
			       VALUES (?, ?, ?, ?, ?)));
$sth_pdf or die;
my $sth_txt = $dbh->prepare(qq(INSERT INTO ParsedDocument
			       VALUES (?, ?, ?, $tom_parser)));

for my $pdf (sort <$pdf_dir/*>)
{
    my ($docnum) = $pdf =~ /(\d+)\.pdf$/;
    if (!defined $docnum)
    {
	warn "No num found for $pdf\n";
	next;
    }
    my $txt = "$txt_dir/$docnum.txt";
    if (! -s $txt)
    {
	warn "No txt found for $pdf\n";
	next;
    }
    open(T, "<", $txt) or die "Cannot open $txt: $!";
    my $line;
    while (<T>)
    {
	chomp;
	s/^\s+//;
	s/\s+$//;
	if ($_)
	{
	    $line = $_;
	    last;
	}
    }
    close(T);
    
    my $doc_id  = Nanoid::generate();
    my $parsed_id  = Nanoid::generate();

    my $sz = -s $pdf;

    $sth_pdf->execute($doc_id, $line, $docnum, $sz, $pdf);
    $sth_txt->execute($parsed_id, $doc_id, $txt);
}
