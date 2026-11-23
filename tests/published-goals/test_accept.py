import copy
import unittest
import accept

class PublicationChecks(unittest.TestCase):
    def receipt(self):
        return {'Path':accept.JOB,'Version':accept.JOB_VERSION,'Sum':accept.JOB_SUM,'GoModSum':accept.JOB_MODSUM,'Origin':{'VCS':'git','URL':'https://'+accept.JOB,'Hash':accept.JOB_HASH}}

    def check(self,receipt):
        accept.check_download(receipt,accept.JOB,accept.JOB_VERSION,accept.JOB_HASH,accept.JOB_SUM,accept.JOB_MODSUM)

    def test_exact_origin_and_both_checksums(self):
        self.check(self.receipt())
        for key,value in (('Path','wrong'),('Version','wrong'),('Sum','h1:wrong'),('GoModSum','h1:wrong'),('Error','not published')):
            with self.subTest(key=key):
                receipt=self.receipt();receipt[key]=value
                with self.assertRaises(ValueError):self.check(receipt)
        for key,value in (('URL','https://example.test/fork'),('Hash','0'*40),('VCS','local')):
            receipt=self.receipt();receipt['Origin'][key]=value
            with self.assertRaises(ValueError):self.check(receipt)
        with self.assertRaises(ValueError):self.check({})

    def test_selected_and_linked_modules_reject_replacements(self):
        expected={accept.JOB:(accept.JOB_VERSION,accept.JOB_SUM)}
        valid=[self.receipt()];accept.check_modules(valid,expected)
        for modules in ([],[dict(valid[0],Version='wrong')],[dict(valid[0],Sum='wrong')],[dict(valid[0],Replace={'Path':'/checkout'})],valid+[{'Path':'other','Replace':{'Path':'local'}}]):
            with self.assertRaises(ValueError):accept.check_modules(modules,expected)

    def test_behavior_requires_every_transport_migrated_and_no_skips(self):
        required=['TestPublishedGoals','TestPublishedGoals/migrated']
        for name in ('direct','default','signed'):
            required += ['TestPublishedGoals/'+name,'TestPublishedGoals/'+name+'/admission','TestPublishedGoals/'+name+'/protected','TestPublishedGoals/migrated/'+name]
        required += ['TestPublishedProject'+name for name in (
            'FreshInstallationAndReopen','CatalogReceiptsAndDeletion','AdministrationAndFirstUse',
            'GuardsAndBorrowedTransactions','HistoricalAdoptionPreservesResources',
            'InstallationRollbackAndInventoryFailures','DamagedGuardsAndResolution',
            'LargeNumericCatalogMetadata','OpaqueCatalogStrings','AdmissionProjectionIntegrity','InstallationConflictsAndExternalInventory',
            'WriterExclusionAndUnknownParticipants','SQLNoopAdmissionAndCopy',
            'SealSurvivesDownstreamFailureAndRelaxedCommit','RefusesIncompleteSharedSchema',
            'RejectsUncommittedSeal','PartialAndFutureInstallations','CommittedSealSurvivesVacuum','CommittedCatalogNewerThanWriter','ConcurrentRetainedCreationAndDeletion','Commands')]
        raw='\n'.join('--- PASS: '+name+' (0.1s)' for name in required)
        accept.check_behavior(raw,0)
        for name in required:
            with self.assertRaises(RuntimeError):accept.check_behavior(raw.replace('--- PASS: '+name+' (','--- ABSENT: '+name+' ('),0)
        for bad in ('--- SKIP: optional (0.1s)','--- FAIL: behavior (0.1s)'):
            with self.assertRaises(RuntimeError):accept.check_behavior(raw+'\n'+bad,0)
        with self.assertRaises(RuntimeError):accept.check_behavior(raw,1)

if __name__=='__main__':unittest.main()
